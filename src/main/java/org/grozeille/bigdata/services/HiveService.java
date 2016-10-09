package org.grozeille.bigdata.services;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang.StringUtils;
import org.grozeille.bigdata.resources.dataset.model.*;
import org.grozeille.bigdata.resources.hive.model.HiveColumn;
import org.grozeille.bigdata.resources.hive.model.HiveColumnStatistics;
import org.grozeille.bigdata.resources.hive.model.HiveDatabase;
import org.grozeille.bigdata.resources.hive.model.HiveTable;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.thrift.TException;
import org.joda.time.DateTime;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

@Service
@Slf4j
public class HiveService {
    public static final String TYPE_NUMERIC_INTEGER = "Numeric Integer";

    public static final String TYPE_NUMERIC_FLOAT = "Numeric Float";

    public static final String TYPE_BOOLEAN = "Boolean";

    public static final String TYPE_DATE_TIME = "Date and Time";

    public static final String TYPE_DATE = "Date";

    public static final String TYPE_STRING = "String";

    public static final String TYPE_BINARY = "Binary";

    public static final String TYPE_STRUCT = "Struct";

    public static final String TYPE_ARRAY = "Array";

    public static final String TYPE_MAP = "Map";

    public static final String TEMP_TABLE_PREFIX = "_datalaketoolbox_cache";

    @Autowired
    private HiveMetaStoreClient hiveMetaStoreClient;

    private ObjectMapper objectMapper = new ObjectMapper();

    private JdbcTemplate hiveJdbcTemplate;

    @Autowired
    @Qualifier("hiveDataSource")
    public void setHiveDataSource(DataSource dataSource){
        hiveJdbcTemplate = new JdbcTemplate(dataSource);

        hiveJdbcTemplate.execute("set tez.session.am.dag.submit.timeout.secs=7200"); // 2h session
        hiveJdbcTemplate.execute("set tez.am.session.min.held-containers=1");
    }

    //@Autowired
    //private HiveContext sparkHiveContext;

    public HiveDatabase[] findAllPublicDatabases() throws TException {

        List<HiveDatabase> result = new ArrayList<>();

        List<String> databases = hiveMetaStoreClient.getAllDatabases();
        for(String db : databases){

            Database database = hiveMetaStoreClient.getDatabase(db);

            HiveDatabase hiveDB = new HiveDatabase();
            hiveDB.setDatabase(db);
            hiveDB.setPath(database.getLocationUri());
            result.add(hiveDB);

        }

        return result.toArray(new HiveDatabase[0]);

    }

    public HiveDatabase findOneDatabase(String database) throws TException {
        try {
            Database db = hiveMetaStoreClient.getDatabase(database);
            HiveDatabase hiveDB = new HiveDatabase();
            hiveDB.setDatabase(database);
            hiveDB.setPath(db.getLocationUri());
            return hiveDB;
        }
        catch (org.apache.hadoop.hive.metastore.api.NoSuchObjectException nsoe){
            return null;
        }
    }

    public HiveTable[] findAllPublicTables() throws TException {

        List<HiveTable> result = new ArrayList<>();

        List<String> databases = hiveMetaStoreClient.getAllDatabases();
        for(String db : databases){

            Database database = hiveMetaStoreClient.getDatabase(db);

            List<String> tables = hiveMetaStoreClient.getAllTables(db);
            for(String t : tables){

                if(!t.startsWith(TEMP_TABLE_PREFIX)) {
                    Table table = hiveMetaStoreClient.getTable(db, t);

                    HiveTable hiveTable = buildHiveTable(table);

                    result.add(hiveTable);
                }
            }

        }
        return result.toArray(new HiveTable[0]);
    }

    public HiveTable findOne(String database, String table) throws TException {
        try {
            Table t = hiveMetaStoreClient.getTable(database, table);
            return buildHiveTable(t);
        }
        catch (org.apache.hadoop.hive.metastore.api.NoSuchObjectException nsoe){
            return null;
        }
    }

    public List<Map<String, Object>> getData(String database, String table, long max){
        return getData("select * from `"+database+"`.`"+table+"` limit "+max);
    }

    public List<Map<String, Object>> getData(DataSetConf dataSetConf, long max) throws HiveQueryException {

        verifyDataSet(dataSetConf);

        String denormalizedSqlQuery = buildDenormalizedSqlQuery(dataSetConf, max, true);

        String sqlQuery = buildSqlQuery(denormalizedSqlQuery, dataSetConf);

        log.info("Executing sql: "+sqlQuery);

        return getData(sqlQuery);
    }

    public List<Map<String, Object>> getData(String sql) {

        /*DataFrame df = sparkHiveContext.sql(sql);
        final String[] fields = df.schema().fieldNames();

        return df.toJavaRDD().map((org.apache.spark.api.java.function.Function<Row, Map<String, Object>>) row -> {
            Map<String, Object> map = new HashMap<>();

            for(int cpt = 0; cpt < fields.length; cpt++){
                map.put(fields[cpt], row.get(cpt));
            }

            return map;
        }).collect();*/

        return hiveJdbcTemplate.queryForList(sql);
    }

    public void createDataSet(DataSetConf dataSetConf) throws HiveQueryException {

        verifyDataSet(dataSetConf);

        String denormalizedSqlQuery = buildDenormalizedSqlQuery(dataSetConf);

        String sqlQuery = buildSqlQuery(denormalizedSqlQuery, dataSetConf);

        String jsonTags = "[]";
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            jsonTags = objectMapper.writeValueAsString(dataSetConf.getTags());
        } catch (JsonProcessingException e) {
            log.warn("Unable to serialize tags", e);
        }

        String createViewSql = "CREATE VIEW `" + dataSetConf.getDatabase() + "`.`" + dataSetConf.getTable() + "`\n" +
                "COMMENT '"+dataSetConf.getComment()+"'\n"+
                "TBLPROPERTIES (\"format\" = \""+dataSetConf.getFormat()+"\", \"tags\" = \""+jsonTags.replace("\"", "\\\"")+"\")\n"+
                "AS "+sqlQuery;

        log.info("Create dataSet: " + createViewSql);

        try {
            hiveJdbcTemplate.execute("DROP VIEW IF EXISTS `"+dataSetConf.getDatabase() + "`.`" + dataSetConf.getTable() + "`");
            hiveJdbcTemplate.execute(createViewSql);
        }catch(Exception e){
            throw new HiveQueryException("Unable to create view: "+sqlQuery, e);
        }
    }

    public void createOrcTable(HiveTable table) throws HiveQueryException {
        String jsonTags = "[]";
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            jsonTags = objectMapper.writeValueAsString(table.getTags());
        } catch (JsonProcessingException e) {
            log.warn("Unable to serialize tags", e);
        }

        String columns = StringUtils.join(Arrays.asList(table.getColumns()).stream()
                .map(c -> "`"+c.getName()+"` STRING COMMENT '"+c.getDescription()+"'")
                .toArray(size -> new String[size]), ",\n");

        String dropSql = "DROP TABLE IF EXISTS `" + table.getDatabase() + "`.`" + table.getTable() + "`";

        String createSql = "CREATE EXTERNAL TABLE `" + table.getDatabase() + "`.`" + table.getTable() + "`\n" +
                "("+columns+")\n"+
                "COMMENT '"+table.getComment()+"'\n"+
                "STORED AS ORC \n"+
                "LOCATION '"+table.getPath()+"'\n"+
                "TBLPROPERTIES (\"format\" = \""+table.getFormat()+"\", \"tags\" = \""+jsonTags.replace("\"", "\\\"")+"\")\n";

        log.info("Drop table: " + dropSql);
        log.info("Create table: " + createSql);

        try {
            hiveJdbcTemplate.execute(dropSql);
            hiveJdbcTemplate.execute(createSql);
        }catch(Exception e){
            throw new HiveQueryException("Unable to create table: "+createSql, e);
        }
    }

    private String buildDenormalizedSqlQuery(DataSetConf dataSetConf) throws HiveQueryException {
        return buildDenormalizedSqlQuery(dataSetConf, Long.MAX_VALUE, false);
    }

    private void verifyDataSet(DataSetConf dataSetConf) throws HiveInvalidDataSetException {
        // verify that column names are unique
        Set<String> columns = new HashSet<>();
        for(DataSetConfTable table : dataSetConf.getTables()){
            for(DataSetConfColumn col : table.getColumns()) {
                if (col.getSelected()) {
                    if(columns.contains(col.getNewName().toLowerCase())){
                        throw new HiveInvalidDataSetException("Column '"+col.getNewName()+"' is duplicated");
                    }
                    columns.add(col.getNewName().toLowerCase());
                }
            }
        }
    }

    private String buildSqlQuery(String denormalizedSqlQuery, DataSetConf dataSetConf) throws HiveInvalidDataSetException {

        // get the primary table, and the others
        DataSetConfTable primaryTable = Arrays.stream(dataSetConf.getTables()).filter(dataSetConfTable -> dataSetConfTable.getPrimary()).findFirst().get();

        // get all tables with links
        Set<String> tables = getLinkedTables(primaryTable.getDatabase(), primaryTable.getTable(), dataSetConf.getLinks());

        // compute the select path
        List<String> selectFirstLevelPartList = new ArrayList<>();
        List<String> selectSecondLevelPartList = new ArrayList<>();

        for(DataSetConfTable table : dataSetConf.getTables()){

            // ignore columns of table not included
            String tableName = formatTableName(table.getDatabase(), table.getTable());
            if(!tables.contains(tableName)){
                continue;
            }

            for(DataSetConfColumn col : table.getColumns()){

                // ignore unselected columns
                if(!col.getSelected()){
                    continue;
                }

                String columnName = "`"+computeColumnAlias(table.getDatabase(), table.getTable(), col.getName())+"`";

                if(!col.getType().equalsIgnoreCase(col.getNewType())){
                    // need a cast
                    String hiveType = "";
                    try{
                        hiveType = getHiveType(col.getNewType());
                    }catch(HiveInvalidDataSetException he){
                        throw new HiveInvalidDataSetException("Unable to cast column "+col.getNewName(), he);
                    }
                    columnName = "CAST("+columnName+" AS "+hiveType+")";
                }

                // include it anyway in the subquery to be used in formula
                selectSecondLevelPartList.add(columnName+" AS `"+col.getNewName().toLowerCase()+"`");
                selectFirstLevelPartList.add("`"+col.getNewName().toLowerCase()+"`");
            }
        }


        for(DataSetConfColumn col : dataSetConf.getCalculatedColumns()){
            String formula = col.getFormula().toLowerCase();
            selectFirstLevelPartList.add(formula+" as `"+col.getNewName().toLowerCase()+"`");
        }

        String sqlQuery = "SELECT "+String.join(" , ", selectFirstLevelPartList)+" FROM ( " +
                "SELECT "+String.join(" , ", selectSecondLevelPartList)+" FROM ("+denormalizedSqlQuery+") L2 " +
                ") L1";
        return sqlQuery;
    }

    private String buildDenormalizedSqlQuery(DataSetConf dataSetConf, long max, boolean buildCache) throws HiveQueryException {
        // get the primary table, and the others
        DataSetConfTable primaryTable = Arrays.stream(dataSetConf.getTables()).filter(dataSetConfTable -> dataSetConfTable.getPrimary()).findFirst().get();
        String primaryTableName = formatTableName(primaryTable.getDatabase(), primaryTable.getTable());


        // compute a unique alias for each tables, excludes tables not in joins
        int tableCpt = 0;
        Map<String, String> tableAliases = new HashMap<>();
        Set<String> tables = getLinkedTables(primaryTable.getDatabase(), primaryTable.getTable(), dataSetConf.getLinks());

        for(DataSetConfTable ds : dataSetConf.getTables()){
            String tableName = formatTableName(ds.getDatabase(), ds.getTable());
            if(tables.contains(tableName)) {
                tableAliases.put(tableName, "T" + (tableCpt++));
            }
            else {
                log.warn("Table "+tableName+" will be ignored because it has no link to it");
            }
        }

        String fromPart = "FROM "+primaryTableName+" AS "+tableAliases.get(primaryTableName);

        // compute the select path
        List<String> selectPartList = new ArrayList<>();

        for(DataSetConfTable table : dataSetConf.getTables()){

            String tableName = formatTableName(table.getDatabase(), table.getTable());
            String tableAlias = tableAliases.get(tableName);

            // ignore columns of table not included
            if(tableAlias == null){
                continue;
            }

            for(DataSetConfColumn col : table.getColumns()){

                String columnName = tableAlias+".`"+col.getName()+"`";
                String columnAlias = computeColumnAlias(table.getDatabase(), table.getTable(), col.getName());

                selectPartList.add(columnName+" AS `"+columnAlias+"`");
            }
        }

        String selectPart = "SELECT "+String.join(" , ", selectPartList);


        // compute the join part
        List<String> joinPartList = new ArrayList<>();
        for(DataSetConfLink link : dataSetConf.getLinks()){
            String rightTableName = formatTableName(link.getRight().getDatabase(), link.getRight().getTable());
            String rightTableAlias = tableAliases.get(rightTableName);

            String leftTableName = formatTableName(link.getLeft().getDatabase(), link.getLeft().getTable());
            String leftTableAlias = tableAliases.get(leftTableName);

            String joinType = link.getType().toUpperCase();
            if(joinType.equalsIgnoreCase("OUTER")){
                joinType = "LEFT OUTER";
            }

            String join = joinType+" JOIN `" + link.getRight().getDatabase()+"`.`"+link.getRight().getTable()+"` AS "+rightTableAlias+" ON ";
            List<String> joinConditions = new ArrayList<>();
            for(DataSetConfLinkColumn lc : link.getColumns()){
                joinConditions.add(leftTableAlias+".`"+lc.getLeft()+"` = "+rightTableAlias+".`"+lc.getRight()+"`");
            }
            join += String.join(" AND ", joinConditions);

            joinPartList.add(join);
        }

        String joinPart = String.join("\n", joinPartList);

        String wherePart = "";
        if(dataSetConf.getFilter().getConditions().length > 0 || dataSetConf.getFilter().getGroups().length > 0){
            wherePart = "WHERE "+computeFilter(dataSetConf, tableAliases, dataSetConf.getFilter());
        }

        String selectStatement = selectPart+"\n"+fromPart+"\n"+joinPart+"\n"+wherePart;

        if(max != Long.MAX_VALUE) {
            selectStatement += "\nLIMIT " + max;
        }

        if(buildCache) {
            selectStatement = buildCache(dataSetConf, tableAliases, selectStatement);
        }

        return selectStatement;
    }

    private String buildCache(DataSetConf dataSetConf, Map<String, String> tableAliases, String selectStatement) throws HiveQueryException {
        // try to get data from cache
        Configuration conf = new Configuration();
        FileSystem fs = null;
        try {
            fs = FileSystem.get(conf);
        } catch (IOException e) {
            throw new HiveQueryException("Unexpected exception", e);
        }

        // get the cached table
        boolean cacheUpToDate = false;

        // compute hash to verify if the cached table is up to date
        String stringToHash = selectStatement;
        //stringToHash += RequestContextHolder.currentRequestAttributes().getSessionId(); // include the session id: not, change each request??
        stringToHash += DateTime.now().toLocalDate().toString(); // invalid for the next day

        String hash = "";
        try {
            MessageDigest cript = MessageDigest.getInstance("SHA-1");
            cript.reset();
            cript.update(stringToHash.getBytes("utf8"));
            hash = new String(Hex.encodeHex(cript.digest()));
        } catch (UnsupportedEncodingException e) {
            throw new HiveQueryException("Unexpected exception", e);
        } catch (NoSuchAlgorithmException e) {
            throw new HiveQueryException("Unexpected exception", e);
        }

        // search for the cached table and check the hash
        String cacheTableName = TEMP_TABLE_PREFIX + dataSetConf.getTable().toLowerCase();
        Table cacheHiveTable = null;
        try {
            cacheHiveTable = hiveMetaStoreClient.getTable(dataSetConf.getDatabase(), cacheTableName);
        } catch (NoSuchObjectException nsoe) {
            log.debug("Table not found: " + cacheTableName, nsoe);
        } catch (MetaException e) {
            throw new HiveQueryException("Unexpected exception", e);
        } catch (TException e) {
            throw new HiveQueryException("Unexpected exception", e);
        }

        final String linkHashKey = "datalaketoolbox_link_hash";

        if (cacheHiveTable != null) {

            String cacheHash = cacheHiveTable.getParameters().get(linkHashKey);

            if (!hash.equalsIgnoreCase(cacheHash)) {
                // cache is not up to date, delete it
                hiveJdbcTemplate.execute("DROP TABLE `" + dataSetConf.getDatabase() + "`.`" + cacheTableName + "`");
            } else {
                cacheUpToDate = true;
            }
        }

        if (!cacheUpToDate) {
            // create the cache, build the query without the cast/rename/formula, with all columns & links
            Path cacheParentPath = new Path(fs.getHomeDirectory(), ".datalake-toolbox");
            Path cacheTablePath = new Path(cacheParentPath, cacheTableName);
            try {
                if (!fs.exists(cacheParentPath)) {
                    fs.mkdirs(cacheParentPath);
                }
                if (!fs.exists(cacheTablePath)) {
                    fs.mkdirs(cacheTablePath);
                }
            } catch (IOException e) {
                throw new HiveQueryException("Unexpected exception", e);
            }



            String createCacheTableSql = "CREATE TABLE `" + dataSetConf.getDatabase() + "`.`" + cacheTableName + "`\n" +
                    "STORED AS ORC\n" +
                    "LOCATION '" + cacheTablePath.toString() + "'\n" +
                    "TBLPROPERTIES ('" + linkHashKey + "'='" + hash + "')\n" +
                    "AS " + selectStatement;

            log.info("Create cache table: " + createCacheTableSql);

            hiveJdbcTemplate.execute(createCacheTableSql);
            //sparkHiveContext.sql(createCacheTableSql).collect();
        }

        List<String> selectCachePartList = new ArrayList<>();

        for(DataSetConfTable table : dataSetConf.getTables()){

            String tableName = formatTableName(table.getDatabase(), table.getTable());
            String tableAlias = tableAliases.get(tableName);

            // ignore columns of table not included
            if(tableAlias == null){
                continue;
            }

            for(DataSetConfColumn col : table.getColumns()){

                String columnAlias = computeColumnAlias(table.getDatabase(), table.getTable(), col.getName());

                selectCachePartList.add("`"+columnAlias+"`");
            }
        }

        String selectCachePart = "SELECT "+String.join(" , ", selectCachePartList);

        // now the select statement is a simple select on the cache table
        selectStatement = selectCachePart + "\nFROM `" + dataSetConf.getDatabase() + "`.`" + cacheTableName + "`";
        return selectStatement;
    }

    private Set<String> getLinkedTables(String database, String table, DataSetConfLink[] links){
        Set<String> result = new HashSet<>();
        result.add(formatTableName(database, table));

        for(DataSetConfLink link : links){
            if(link.getLeft().getDatabase().equalsIgnoreCase(database) && link.getLeft().getTable().equalsIgnoreCase(table)){
                result.addAll(getLinkedTables(link.getRight().getDatabase(), link.getRight().getTable(), links));
            }
        }

        return result;
    }

    private String getHiveType(String dataSetType) throws HiveInvalidDataSetException {
        if(dataSetType == null){
            throw new HiveInvalidDataSetException("No type");
        }

        switch (dataSetType){
            case TYPE_NUMERIC_INTEGER:
                return "BIGINT";
            case TYPE_NUMERIC_FLOAT:
                return "DOUBLE";
            case TYPE_BOOLEAN:
                return "BOOLEAN";
            case TYPE_DATE_TIME:
                return "TIMESTAMP";
            case TYPE_DATE:
                return "DATE";
            case TYPE_STRING:
                return "STRING";
        }

        throw new HiveInvalidDataSetException("Unknown type "+dataSetType);
    }

    private String computeFilter(DataSetConf dataSetConf, Map<String, String> tableAliases, DataSetFilterGroup group) throws HiveInvalidDataSetException {
        String operator = group.getOperator();

        List<String> conditions = new ArrayList<>();
        for(DataSetFilterCondition c : group.getConditions()){

            String columnName = "";
            String columnType = "";

            // search for database
            DataSetConfTable table = Arrays.stream(dataSetConf.getTables()).filter(dataSetConfTable ->
                    dataSetConfTable.getDatabase().equalsIgnoreCase(c.getDatabase()) && dataSetConfTable.getTable().equalsIgnoreCase(c.getTable())
            ).findFirst().get();

            // search for table
            DataSetConfColumn column = Arrays.stream(table.getColumns()).filter(dataSetConfColumn ->
                    dataSetConfColumn.getName().equalsIgnoreCase(c.getColumn())
            ).findFirst().get();

            // compute column name
            String tableName = formatTableName(table.getDatabase(), table.getTable());
            String tableAlias = tableAliases.get(tableName);

            columnName = "`" + tableAlias + "`.`" + column.getName() + "`";

            columnType = column.getType();

            if(!column.getType().equalsIgnoreCase(column.getNewType())){
                // need a cast
                columnType = column.getNewType();
                String hiveType = "";
                try{
                    hiveType = getHiveType(column.getNewType());
                }catch(HiveInvalidDataSetException he){
                    throw new HiveInvalidDataSetException("Unable to cast column "+c.getColumn(), he);
                }
                columnName = "CAST("+columnName+" AS "+hiveType+")";
            }

            String hiveTestValue = "";
            try {
                switch (columnType) {
                    case TYPE_NUMERIC_INTEGER:
                        long longValue = Long.parseLong(c.getData());
                        hiveTestValue = Long.toString(longValue);
                        break;
                    case TYPE_NUMERIC_FLOAT:
                        double doubleValue = Double.parseDouble(c.getData());
                        hiveTestValue = Double.toString(doubleValue);
                        break;
                    case TYPE_BOOLEAN:
                        boolean booleanValue = Boolean.parseBoolean(c.getData());
                        hiveTestValue = Boolean.toString(booleanValue).toUpperCase();
                        break;
                    case TYPE_DATE_TIME:
                        hiveTestValue = "'" + c.getData() + "'";
                        break;
                    case TYPE_DATE:
                        hiveTestValue = "'" + c.getData() + "'";
                        break;
                    case TYPE_STRING:
                        hiveTestValue = "'" + c.getData() + "'";
                        break;
                }
            }
            catch(Exception e){
                throw new HiveInvalidDataSetException("Filter value has wrong type", e);
            }

            String hiveCondition = "";
            switch (c.getCondition()) {
                case "=":
                case "!=":
                case "<":
                case "<=":
                case ">":
                case ">=":
                case "IS NULL":
                case "IS NOT NULL":
                    hiveCondition = c.getCondition();
                    break;
                case "BEGINS WITH":
                    hiveCondition = "LIKE";
                    hiveTestValue = "'" + c.getData() + "%'";
                    break;
                case "NOT BEGINS WITH":
                    hiveCondition = "NOT LIKE";
                    hiveTestValue = "'" + c.getData() + "%'";
                    break;
                case "CONTAINS":
                    hiveCondition = "LIKE";
                    hiveTestValue = "'%" + c.getData() + "%'";
                    break;
                case "NOT CONTAINS":
                    hiveCondition = "NOT LIKE";
                    hiveTestValue = "'%" + c.getData() + "%'";
                    break;
                case "ENDS WITH":
                    hiveCondition = "LIKE";
                    hiveTestValue = "'%" + c.getData() + "'";
                    break;
                case "NOT ENDS WITH":
                    hiveCondition = "NOT LIKE";
                    hiveTestValue = "'%" + c.getData() + "'";
                    break;
                case "IN":
                case "NOT IN":
                    hiveCondition = c.getCondition();
                    break;
            }

            conditions.add(columnName+" "+hiveCondition+" "+hiveTestValue);

        }

        for(DataSetFilterGroup g : group.getGroups()) {
            conditions.add(computeFilter(dataSetConf, tableAliases, g));
        }

        String result = "("+String.join(" "+operator+" ", conditions)+")";
        return result;
    }

    private String formatTableName(String database, String table){
        return "`"+database+"`.`"+table+"`";
    }

    private String computeColumnAlias(String database, String table, String column){
        return (database+"_"+table+"_"+column).toLowerCase();
    }

    private HiveTable buildHiveTable(Table table) throws TException {
        HiveTable hiveTable = new HiveTable();
        hiveTable.setDatabase(table.getDbName());
        hiveTable.setTable(table.getTableName());
        hiveTable.setComment(table.getParameters().getOrDefault("comment", ""));
        hiveTable.setDataDomainOwner(table.getOwner());
        hiveTable.setFormat(table.getParameters().getOrDefault("format", ""));
        // TODO format view ?
        String tagsJson = table.getParameters().getOrDefault("tags", "[]");
        String[] tags = new String[0];
        try {
            tags = objectMapper.readValue(tagsJson, String[].class);
        } catch (IOException e) {
            log.warn("Unable to parse tags for table "+table.getDbName()+"."+table.getTableName(), e);
        }
        hiveTable.setTags(tags);
        try {
            if(table.getSd().getLocation() != null) {
                URI hdfsUri = new URI(table.getSd().getLocation());
                hiveTable.setPath(hdfsUri.getPath());
            }
            else {
                hiveTable.setPath("");
            }
        } catch (URISyntaxException e) {
            log.warn("Unable to parse uri for location: "+table.getSd().getLocation());
            hiveTable.setPath(table.getSd().getLocation());
        }

        Long totalSize = 0l;
        Long totalRows = 0l;

        if(table.getParameters().get("COLUMN_STATS_ACCURATE") != null && table.getParameters().get("COLUMN_STATS_ACCURATE").equalsIgnoreCase("true")){

            if(table.getParameters().get("totalSize") != null){
                totalSize += Long.parseLong(table.getParameters().get("totalSize"));
            }

            if(table.getParameters().get("numRows") != null){
                totalRows += Long.parseLong(table.getParameters().get("numRows"));

            }
        }

        List<HiveColumn> columns = new ArrayList<>();
        List<FieldSchema> fields = hiveMetaStoreClient.getSchema(table.getDbName(), table.getTableName());
        List<String> columnNames = new ArrayList<>();
        Map<String, HiveColumnStatistics> hiveColumnStatisticsMap = new HashMap<>();

        for(FieldSchema f : fields){
            HiveColumn column = new HiveColumn();
            column.setName(f.getName());
            column.setDescription(f.getComment());
            if(f.getType().startsWith("struct")){
                column.setType(TYPE_STRUCT);
            }
            else if(f.getType().startsWith("array")){
                column.setType(TYPE_ARRAY);
            }
            else if(f.getType().startsWith("map")){
                column.setType(TYPE_MAP);
            }
            else {
                switch(f.getType()){
                    case "tinyint":
                    case "smallint":
                    case "int":
                    case "bigint":
                        column.setType(TYPE_NUMERIC_INTEGER);
                        break;
                    case "float":
                    case "double":
                    case "decimal":
                        column.setType(TYPE_NUMERIC_FLOAT);
                        break;
                    case "string":
                    case "varchar":
                    case "char":
                        column.setType(TYPE_STRING);
                        break;
                    case "boolean":
                        column.setType(TYPE_BOOLEAN);
                        break;
                    case "date":
                        column.setType(TYPE_DATE);
                    case "timestamp":
                        column.setType(TYPE_DATE_TIME);
                        break;
                    case "binary":
                        column.setType(TYPE_BINARY);
                        break;
                }
            }
            column.setStatistics(new HiveColumnStatistics());
            hiveColumnStatisticsMap.put(f.getName(), column.getStatistics());
            columns.add(column);

            columnNames.add(f.getName());
        }

        List<ColumnStatisticsObj> statisticsObjs = hiveMetaStoreClient.getTableColumnStatistics(table.getDbName(), table.getTableName(), columnNames);
        for(ColumnStatisticsObj s : statisticsObjs){
            HiveColumnStatistics hiveColumnStatistics = hiveColumnStatisticsMap.get(s.getColName());
            switch(s.getColType()){
                case "tinyint":
                case "smallint":
                case "int":
                case "bigint":
                    hiveColumnStatistics.setMin(Long.toString(s.getStatsData().getLongStats().getLowValue()));
                    hiveColumnStatistics.setMax(Long.toString(s.getStatsData().getLongStats().getHighValue()));
                    hiveColumnStatistics.setCount(Long.toString(totalRows-s.getStatsData().getLongStats().getNumNulls()));
                    break;
                case "float":
                case "double":
                    hiveColumnStatistics.setMin(Double.toString(s.getStatsData().getDoubleStats().getLowValue()));
                    hiveColumnStatistics.setMax(Double.toString(s.getStatsData().getDoubleStats().getHighValue()));
                    hiveColumnStatistics.setCount(Long.toString(totalRows-s.getStatsData().getDoubleStats().getNumNulls()));
                    break;
                case "decimal":
                    hiveColumnStatistics.setMin(s.getStatsData().getDecimalStats().getLowValue().toString());
                    hiveColumnStatistics.setMax(s.getStatsData().getDecimalStats().getHighValue().toString());
                    hiveColumnStatistics.setCount(Long.toString(totalRows-s.getStatsData().getDecimalStats().getNumNulls()));
                    break;
                case "string":
                case "varchar":
                case "char":
                    hiveColumnStatistics.setMax("length "+ Long.toString(s.getStatsData().getStringStats().getMaxColLen()));
                    hiveColumnStatistics.setCount(Long.toString(totalRows-s.getStatsData().getStringStats().getNumNulls()));
                    break;
                case "boolean":
                    hiveColumnStatistics.setMin("false "+Long.toString(s.getStatsData().getBooleanStats().getNumFalses()));
                    hiveColumnStatistics.setMax("true "+Long.toString(s.getStatsData().getBooleanStats().getNumTrues()));
                    hiveColumnStatistics.setCount(Long.toString(totalRows-s.getStatsData().getBooleanStats().getNumNulls()));
                    break;
                case "date":
                case "timestamp":
                    hiveColumnStatistics.setMin(s.getStatsData().getDateStats().getLowValue().toString());
                    hiveColumnStatistics.setMax(s.getStatsData().getDateStats().getHighValue().toString());
                    hiveColumnStatistics.setCount(Long.toString(totalRows-s.getStatsData().getDateStats().getNumNulls()));
                    break;
                case "binary":
                    hiveColumnStatistics.setMax("length "+ Long.toString(s.getStatsData().getBinaryStats().getMaxColLen()));
                    hiveColumnStatistics.setCount(Long.toString(totalRows-s.getStatsData().getBinaryStats().getNumNulls()));
                    break;
            }

        }

        hiveTable.setColumns(columns.toArray(new HiveColumn[0]));

        return hiveTable;
    }


    /*
    data_type
  : primitive_type
  | array_type
  | map_type
  | struct_type
  | union_type  -- (Note: Available in Hive 0.7.0 and later)

primitive_type
  : TINYINT
  | SMALLINT
  | INT
  | BIGINT
  | BOOLEAN
  | FLOAT
  | DOUBLE
  | STRING
  | BINARY      -- (Note: Available in Hive 0.8.0 and later)
  | TIMESTAMP   -- (Note: Available in Hive 0.8.0 and later)
  | DECIMAL     -- (Note: Available in Hive 0.11.0 and later)
  | DECIMAL(precision, scale)  -- (Note: Available in Hive 0.13.0 and later)
  | DATE        -- (Note: Available in Hive 0.12.0 and later)
  | VARCHAR     -- (Note: Available in Hive 0.12.0 and later)
  | CHAR        -- (Note: Available in Hive 0.13.0 and later)
     */
}
