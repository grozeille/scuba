package org.grozeille.bigdata.dataset.services;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;
import org.grozeille.bigdata.dataset.exceptions.HiveDatabaseNotFoundException;
import org.grozeille.bigdata.dataset.exceptions.HiveInvalidDataSetException;
import org.grozeille.bigdata.dataset.exceptions.HiveQueryException;
import org.grozeille.bigdata.dataset.exceptions.HiveTableNotFoundException;
import org.grozeille.bigdata.dataset.model.*;
import org.grozeille.bigdata.dataset.model.HiveDatabase;
import org.grozeille.bigdata.dataset.model.HiveTable;
import org.joda.time.DateTime;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

@Service
@Slf4j
public class WranglingDataSetService {

    @Autowired
    private HiveService hiveService;

    @Autowired
    private DataSetService dataSetService;

    @Autowired
    private HiveMetaStoreClient hiveMetaStoreClient;

    private final ObjectMapper objectMapper = new ObjectMapper();

    private JdbcTemplate hiveJdbcTemplate;

    @Autowired
    @Qualifier("hiveDataSource")
    public void setHiveDataSource(DataSource dataSource){
        hiveJdbcTemplate = new JdbcTemplate(dataSource);
    }

    public void createOrUpdateWranglingView(
            DataSetConf dataSetConf,
            String creator,
            Boolean temporary,
            WranglingDataSetConf wranglingDataSetConf) throws Exception {

        verifyDataSet(wranglingDataSetConf);

        // do all joins
        String denormalizedSqlQuery = buildDenormalizedSqlQuery(
                dataSetConf.getDatabase(),
                dataSetConf.getTable(),
                creator,
                wranglingDataSetConf);

        // do all filters, type conversion, column renaming, etc.
        String sqlQuery = buildSqlQuery(wranglingDataSetConf, denormalizedSqlQuery);

        String json = objectMapper.writeValueAsString(wranglingDataSetConf);
        String dataSetType = DataSetType.CustomFileDataSet.name();

        HiveDatabase hiveDatabase = hiveService.findOneDatabase(dataSetConf.getDatabase());
        if(hiveDatabase == null) {
            throw new HiveDatabaseNotFoundException(dataSetConf.getDatabase() + " not found");
        }

        HiveTable hiveTable = hiveService.findOne(dataSetConf.getDatabase(), dataSetConf.getTable());

        if(hiveTable == null) {

            hiveTable = new HiveTable();
            hiveTable.setDatabase(dataSetConf.getDatabase());
            hiveTable.setTable(dataSetConf.getTable());
            hiveTable.setComment(dataSetConf.getComment());
            hiveTable.setTags(dataSetConf.getTags());
            hiveTable.setCreator(creator);
            hiveTable.setTemporary(temporary);
            hiveTable.setDataSetType(dataSetType);
            hiveTable.setDataSetConfiguration(json);

            String tablePath = hiveDatabase.getPath() + "/" + dataSetConf.getTable();
            hiveTable.setPath(tablePath);

            hiveService.createView(hiveTable, sqlQuery);
        }
        else {
            hiveTable.setComment(dataSetConf.getComment());
            hiveTable.setTags(dataSetConf.getTags());
            hiveTable.setCreator(creator);
            hiveTable.setDataSetConfiguration(json);
            hiveTable.setTemporary(temporary);

            hiveService.updateView(hiveTable);
        }

        if(!temporary) {
            dataSetService.refreshTable(dataSetConf.getDatabase(), dataSetConf.getTable());
        }
    }

    public List<Map<String, Object>> getPreviewData(
            String database,
            String table,
            long max) throws Exception {

        HiveTable hiveTable = hiveService.findOne(database, table);

        if(hiveTable == null) {
            throw new HiveTableNotFoundException(database + "." + table + " not found");
        }

        WranglingDataSetConf wranglingDataSetConf = extractDataSetConf(hiveTable);

        verifyDataSet(wranglingDataSetConf);

        String denormalizedSqlQuery = buildDenormalizedSqlQuery(database, table, hiveTable.getCreator(), wranglingDataSetConf, max, true);

        String sqlQuery = buildSqlQuery(wranglingDataSetConf, denormalizedSqlQuery);

        return hiveService.getData(sqlQuery, true);
    }

    public WranglingDataSetConf extractDataSetConf(HiveTable hiveTable) throws java.io.IOException {
        return objectMapper.readValue(hiveTable.getDataSetConfiguration(), WranglingDataSetConf.class);
    }

    private void verifyDataSet(WranglingDataSetConf wranglingDataSetConf) throws HiveInvalidDataSetException {
        // verify that column names are unique
        Set<String> columns = new HashSet<>();
        for(WranglingDataSetConfTable table : wranglingDataSetConf.getTables()){
            for(WranglingDataSetConfColumn col : table.getColumns()) {
                if (col.getSelected()) {
                    if(columns.contains(col.getNewName().toLowerCase())){
                        throw new HiveInvalidDataSetException("Column '"+col.getNewName()+"' is duplicated");
                    }
                    columns.add(col.getNewName().toLowerCase());
                }
            }
        }
    }

    private String buildDenormalizedSqlQuery(
            String database,
            String table,
            String creator,
            WranglingDataSetConf wranglingDataSetConf) throws HiveQueryException {

        return buildDenormalizedSqlQuery(
                database,
                table,
                creator,
                wranglingDataSetConf,
                Long.MAX_VALUE,
                false);
    }

    private String buildDenormalizedSqlQuery(
            String database,
            String table,
            String creator,
            WranglingDataSetConf wranglingDataSetConf,
            long max,
            boolean buildCache) throws HiveQueryException {

        // get the primary table, and the others
        WranglingDataSetConfTable primaryTable = Arrays.stream(wranglingDataSetConf.getTables())
                .filter(dataSetConfTable -> dataSetConfTable.getPrimary())
                .findFirst()
                .get();
        String primaryTableName = formatTableName(primaryTable.getDatabase(), primaryTable.getTable());


        // compute a unique alias for each tables, excludes tables not in joins
        int tableCpt = 0;
        Map<String, String> tableAliases = new HashMap<>();
        Set<String> tables = getLinkedTables(primaryTable.getDatabase(), primaryTable.getTable(), wranglingDataSetConf.getLinks());

        for(WranglingDataSetConfTable ds : wranglingDataSetConf.getTables()){
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

        for(WranglingDataSetConfTable wranglingTable : wranglingDataSetConf.getTables()){

            String tableName = formatTableName(wranglingTable.getDatabase(), wranglingTable.getTable());
            String tableAlias = tableAliases.get(tableName);

            // ignore columns of table not included
            if(tableAlias == null){
                continue;
            }

            for(WranglingDataSetConfColumn col : wranglingTable.getColumns()){

                String columnName = tableAlias+".`"+col.getName()+"`";
                String columnAlias = computeColumnAlias(wranglingTable.getDatabase(), wranglingTable.getTable(), col.getName());

                selectPartList.add(columnName+" AS `"+columnAlias+"`");
            }
        }

        String selectPart = "SELECT "+String.join(" , ", selectPartList);


        // compute the join part
        List<String> joinPartList = new ArrayList<>();
        for(WranglingDataSetConfLink link : wranglingDataSetConf.getLinks()){
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
            for(WranglingDataSetConfLinkColumn lc : link.getColumns()){
                joinConditions.add(leftTableAlias+".`"+lc.getLeft()+"` = "+rightTableAlias+".`"+lc.getRight()+"`");
            }
            join += String.join(" AND ", joinConditions);

            joinPartList.add(join);
        }

        String joinPart = String.join("\n", joinPartList);

        String wherePart = "";
        if(wranglingDataSetConf.getFilter().getConditions().length > 0 || wranglingDataSetConf.getFilter().getGroups().length > 0){
            wherePart = "WHERE "+computeFilter(wranglingDataSetConf, tableAliases, wranglingDataSetConf.getFilter());
        }

        String selectStatement = selectPart+"\n"+fromPart+"\n"+joinPart+"\n"+wherePart;

        if(max != Long.MAX_VALUE) {
            selectStatement += "\nLIMIT " + max;
        }

        if(buildCache) {
            selectStatement = buildCache(database, table, creator, wranglingDataSetConf, tableAliases, selectStatement);
        }

        return selectStatement;
    }

    private String buildSqlQuery(WranglingDataSetConf wranglingDataSetConf, String denormalizedSqlQuery) throws HiveInvalidDataSetException {

        // get the primary table, and the others
        WranglingDataSetConfTable primaryTable = Arrays.stream(wranglingDataSetConf.getTables()).filter(dataSetConfTable -> dataSetConfTable.getPrimary()).findFirst().get();

        // get all tables with links
        Set<String> tables = getLinkedTables(primaryTable.getDatabase(), primaryTable.getTable(), wranglingDataSetConf.getLinks());

        // compute the select path
        List<String> selectFirstLevelPartList = new ArrayList<>();
        List<String> selectSecondLevelPartList = new ArrayList<>();

        for(WranglingDataSetConfTable table : wranglingDataSetConf.getTables()){

            // ignore columns of table not included
            String tableName = formatTableName(table.getDatabase(), table.getTable());
            if(!tables.contains(tableName)){
                continue;
            }

            for(WranglingDataSetConfColumn col : table.getColumns()){

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


        for(WranglingDataSetConfColumn col : wranglingDataSetConf.getCalculatedColumns()){
            String formula = col.getFormula().toLowerCase();
            selectFirstLevelPartList.add(formula+" as `"+col.getNewName().toLowerCase()+"`");
        }

        String sqlQuery = "SELECT "+String.join(" , ", selectFirstLevelPartList)+" FROM ( " +
                "SELECT "+String.join(" , ", selectSecondLevelPartList)+" FROM ("+denormalizedSqlQuery+") L2 " +
                ") L1";
        return sqlQuery;
    }

    private String buildCache(
            String database,
            String table,
            String creator,
            WranglingDataSetConf wranglingDataSetConf,
            Map<String, String> tableAliases, String selectStatement) throws HiveQueryException {

        // get the cached table
        boolean cacheUpToDate = false;

        // compute hash to verify if the cached table is up to date
        String stringToHash = selectStatement;
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
        String cacheTableName = database + "_tmp_cache_" + creator + "_" + table;
        Table cacheHiveTable = null;
        try {
            cacheHiveTable = hiveMetaStoreClient.getTable(database, cacheTableName);
        } catch (NoSuchObjectException nsoe) {
            log.debug("Table not found: " + cacheTableName, nsoe);
        } catch (MetaException e) {
            throw new HiveQueryException("Unexpected exception", e);
        } catch (TException e) {
            throw new HiveQueryException("Unexpected exception", e);
        }

        final String cacheHashKey = "cache_hash";

        if (cacheHiveTable != null) {

            String cacheHash = cacheHiveTable.getParameters().get(cacheHashKey);

            if (!hash.equalsIgnoreCase(cacheHash)) {
                // cache is not up to date, deleteTable it
                hiveJdbcTemplate.execute("DROP TABLE `" + database + "`.`" + cacheTableName + "`");
            } else {
                cacheUpToDate = true;
            }
        }

        if (!cacheUpToDate) {
            // create the cache, build the query without the cast/rename/formula, with all columns & links

            String createCacheTableSql = "CREATE TABLE `" + database + "`.`" + cacheTableName + "`\n" +
                    "STORED AS ORC\n" +
                    "TBLPROPERTIES ('" + cacheHashKey + "'='" + hash + "')\n" +
                    "AS " + selectStatement;

            log.info("Creating cache table: " + createCacheTableSql);

            long startTime = System.currentTimeMillis();

            hiveJdbcTemplate.execute(createCacheTableSql);

            log.info("SQL executed in: " + (System.currentTimeMillis() - startTime)+" ms");

            //sparkHiveContext.sql(createCacheTableSql).collect();
        }

        List<String> selectCachePartList = new ArrayList<>();

        for(WranglingDataSetConfTable wranglingTable : wranglingDataSetConf.getTables()){

            String tableName = formatTableName(wranglingTable.getDatabase(), wranglingTable.getTable());
            String tableAlias = tableAliases.get(tableName);

            // ignore columns of table not included
            if(tableAlias == null){
                continue;
            }

            for(WranglingDataSetConfColumn col : wranglingTable.getColumns()){

                String columnAlias = computeColumnAlias(wranglingTable.getDatabase(), wranglingTable.getTable(), col.getName());

                selectCachePartList.add("`"+columnAlias+"`");
            }
        }

        String selectCachePart = "SELECT "+String.join(" , ", selectCachePartList);

        // now the select statement is a simple select on the cache table
        selectStatement = selectCachePart + "\nFROM `" + database + "`.`" + cacheTableName + "`";
        return selectStatement;
    }

    private Set<String> getLinkedTables(String database, String table, WranglingDataSetConfLink[] links){
        Set<String> result = new HashSet<>();
        result.add(formatTableName(database, table));

        for(WranglingDataSetConfLink link : links){
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
            case HiveService.TYPE_NUMERIC_INTEGER:
                return "BIGINT";
            case HiveService.TYPE_NUMERIC_FLOAT:
                return "DOUBLE";
            case HiveService.TYPE_BOOLEAN:
                return "BOOLEAN";
            case HiveService.TYPE_DATE_TIME:
                return "TIMESTAMP";
            case HiveService.TYPE_DATE:
                return "DATE";
            case HiveService.TYPE_STRING:
                return "STRING";
        }

        throw new HiveInvalidDataSetException("Unknown type "+dataSetType);
    }

    private String computeFilter(WranglingDataSetConf wranglingDataSetConf, Map<String, String> tableAliases, WranglingDataSetFilterGroup group) throws HiveInvalidDataSetException {
        String operator = group.getOperator();

        List<String> conditions = new ArrayList<>();
        for(WranglingDataSetFilterCondition c : group.getConditions()){

            String columnName = "";
            String columnType = "";

            // search for database
            WranglingDataSetConfTable table = Arrays.stream(wranglingDataSetConf.getTables()).filter(dataSetConfTable ->
                    dataSetConfTable.getDatabase().equalsIgnoreCase(c.getDatabase()) && dataSetConfTable.getTable().equalsIgnoreCase(c.getTable())
            ).findFirst().get();

            // search for table
            WranglingDataSetConfColumn column = Arrays.stream(table.getColumns()).filter(dataSetConfColumn ->
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
                    case HiveService.TYPE_NUMERIC_INTEGER:
                        long longValue = Long.parseLong(c.getData());
                        hiveTestValue = Long.toString(longValue);
                        break;
                    case HiveService.TYPE_NUMERIC_FLOAT:
                        double doubleValue = Double.parseDouble(c.getData());
                        hiveTestValue = Double.toString(doubleValue);
                        break;
                    case HiveService.TYPE_BOOLEAN:
                        boolean booleanValue = Boolean.parseBoolean(c.getData());
                        hiveTestValue = Boolean.toString(booleanValue).toUpperCase();
                        break;
                    case HiveService.TYPE_DATE_TIME:
                        hiveTestValue = "'" + c.getData() + "'";
                        break;
                    case HiveService.TYPE_DATE:
                        hiveTestValue = "'" + c.getData() + "'";
                        break;
                    case HiveService.TYPE_STRING:
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

        for(WranglingDataSetFilterGroup g : group.getGroups()) {
            conditions.add(computeFilter(wranglingDataSetConf, tableAliases, g));
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

}
