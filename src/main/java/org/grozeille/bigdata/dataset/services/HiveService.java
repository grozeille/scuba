package org.grozeille.bigdata.dataset.services;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.grozeille.bigdata.dataset.exceptions.HiveQueryException;
import org.grozeille.bigdata.dataset.model.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.thrift.TException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.ColumnMapRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Stream;

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

    private final List<String> reservedKeys = Arrays.asList(
            "format",
            "creator",
            "comment",
            "temporary",
            "tags",
            "dataSetType",
            "dataSetConfigPath");

    @Autowired
    private HiveMetaStoreClient hiveMetaStoreClient;

    private ObjectMapper objectMapper = new ObjectMapper();

    private JdbcTemplate hiveJdbcTemplate;

    @Autowired
    private FileSystem fs;

    @Autowired
    private HdfsService hdfsService;

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

        // TODO : specify a regex filter ?

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

    public Stream<HiveTable> findAllPublicTables() throws TException {

        // TODO : specify a regex filter ?

        List<HiveTable> result = new ArrayList<>();

        // get all tables from Hive
        HiveDatabase[] databases = this.findAllPublicDatabases();
        Stream<HiveTable> tableStream = Arrays.asList(databases).stream().map(db -> db.getDatabase()).flatMap(db -> {
            try {
                List<String> tables = hiveMetaStoreClient.getAllTables(db);

                return tables.stream().map(t -> {
                    try {
                        Table table = hiveMetaStoreClient.getTable(db, t);
                        HiveTable hiveTable = buildHiveTable(table);
                        return hiveTable;
                    } catch (TException e) {
                        log.error("Unable to get details for table: "+db+"."+t);
                        HiveTable hiveTable = new HiveTable();
                        hiveTable.setDatabase(db);
                        hiveTable.setTable(t);
                        return hiveTable;
                    }
                });

            } catch (MetaException e) {
                log.error("Unable to get tables for db: "+db);
                return new ArrayList<HiveTable>().stream();
            }

        }).filter(Objects::nonNull);

        return tableStream;
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

    public List<Map<String, Object>> getData(String database, String table, long max, Boolean useTablePrefix){
        return getData("select * from `"+database+"`.`"+table+"` limit "+max, useTablePrefix);
    }

    public List<Map<String, Object>> getData(String sql, Boolean useTablePrefix) {

        /*DataFrame df = sparkHiveContext.sql(sql);
        final String[] fields = df.schema().fieldNames();

        return df.toJavaRDD().map((org.apache.spark.api.java.function.Function<Row, Map<String, Object>>) row -> {
            Map<String, Object> map = new HashMap<>();

            for(int cpt = 0; cpt < fields.length; cpt++){
                map.put(fields[cpt], row.get(cpt));
            }

            return map;
        }).collect();*/

        log.info("Executing SQL: " + sql);
        long startTime = System.currentTimeMillis();

        RowMapper<Map<String, Object>> rowMapper;
        if(useTablePrefix) {
            rowMapper = new ColumnMapRowMapper();
        }
        else {
            rowMapper = new ColumnMapRowMapper() {
                protected String getColumnKey(String columnName) {
                    String[] split = columnName.split("\\.", 2);
                    if(split.length > 1) {
                        return split[1];
                    }
                    else {
                        return split[0];
                    }
                }
            };
        }

        List<Map<String, Object>> result = hiveJdbcTemplate.query(sql, rowMapper);

        log.info("SQL executed in: " + (System.currentTimeMillis() - startTime)+" ms");

        return result;
    }

    public void createOrcTable(HiveTable table) throws HiveQueryException, IOException, TException {

        deleteTable(table);

        String previousSchema = getCurrentSchema(table);

        String columns = StringUtils.join(Arrays.asList(table.getColumns()).stream()
                .map(c -> "`"+c.getName()+"` STRING COMMENT '"+c.getDescription()+"'")
                .toArray(size -> new String[size]), ",\n");

        InputStream configInputStream = new ByteArrayInputStream(table.getDataSetConfiguration().getBytes(StandardCharsets.UTF_8));
        HdfsService.HdfsFileInfo dataSetConfigfileInfo = hdfsService.write(configInputStream, "datasetconfig.json", table.getPath());

        String createSql = "CREATE EXTERNAL TABLE `" + table.getDatabase() + "`.`" + table.getTable() + "`\n" +
                "("+columns+")\n"+
                "STORED AS ORC \n"+
                "LOCATION '"+table.getPath()+"'\n"+
                buildTableProperties(table, dataSetConfigfileInfo);

        log.info("Create table: " + createSql);

        try {
            long startTime = System.currentTimeMillis();
            hiveJdbcTemplate.execute(createSql);
            log.info("SQL executed in: " + (System.currentTimeMillis() - startTime)+" ms");
        }catch(Exception e){
            if(previousSchema != null) {
                hiveJdbcTemplate.execute(previousSchema);
            }
            throw new HiveQueryException("Unable to create table: "+createSql, e);
        }
    }

    public void createView(HiveTable table, String sqlQuery) throws HiveQueryException, IOException, TException {

        deleteTable(table);

        String previousSchema = getCurrentSchema(table);

        InputStream configInputStream = new ByteArrayInputStream(table.getDataSetConfiguration().getBytes(StandardCharsets.UTF_8));
        HdfsService.HdfsFileInfo dataSetConfigfileInfo = hdfsService.write(configInputStream, "datasetconfig.json", table.getPath());

        String createSql = "CREATE VIEW `" + table.getDatabase() + "`.`" + table.getTable() + "`\n" +
                buildTableProperties(table, dataSetConfigfileInfo) +"\n"+
                "AS "+sqlQuery;

        log.info("Create view: " + createSql);

        try {
            long startTime = System.currentTimeMillis();
            hiveJdbcTemplate.execute(createSql);
            log.info("SQL executed in: " + (System.currentTimeMillis() - startTime)+" ms");
        }catch(Exception e){
            if(previousSchema != null) {
                hiveJdbcTemplate.execute(previousSchema);
            }
            throw new HiveQueryException("Unable to create view: "+createSql, e);
        }
    }

    public void updateTable(HiveTable table) throws HiveQueryException, IOException {

        InputStream configInputStream = new ByteArrayInputStream(table.getDataSetConfiguration().getBytes(StandardCharsets.UTF_8));
        HdfsService.HdfsFileInfo dataSetConfigfileInfo = hdfsService.write(configInputStream, "datasetconfig.json", table.getPath());

        String alterPropertiesSql = "ALTER TABLE `" + table.getDatabase() + "`.`" + table.getTable() + "`\n" +
                "SET "+ buildTableProperties(table, dataSetConfigfileInfo);

        log.info("Alter table properties: " + alterPropertiesSql);

        try {
            long startTime = System.currentTimeMillis();
            hiveJdbcTemplate.execute(alterPropertiesSql);
            log.info("SQL executed in: " + (System.currentTimeMillis() - startTime)+" ms");
        }catch(Exception e){
            throw new HiveQueryException("Unable to alter table: "+alterPropertiesSql, e);
        }
    }

    public void updateView(HiveTable table) throws HiveQueryException, IOException {

        InputStream configInputStream = new ByteArrayInputStream(table.getDataSetConfiguration().getBytes(StandardCharsets.UTF_8));
        HdfsService.HdfsFileInfo dataSetConfigfileInfo = hdfsService.write(configInputStream, "datasetconfig.json", table.getPath());

        String alterPropertiesSql = "ALTER VIEW `" + table.getDatabase() + "`.`" + table.getTable() + "`\n" +
                "SET "+ buildTableProperties(table, dataSetConfigfileInfo);

        log.info("Alter view properties: " + alterPropertiesSql);

        try {
            long startTime = System.currentTimeMillis();
            hiveJdbcTemplate.execute(alterPropertiesSql);
            log.info("SQL executed in: " + (System.currentTimeMillis() - startTime)+" ms");
        }catch(Exception e){
            throw new HiveQueryException("Unable to alter view: "+alterPropertiesSql, e);
        }
    }

    public String getCurrentSchema(HiveTable table) throws HiveQueryException, TException {
        HiveTable previous = this.findOne(table.getDatabase(), table.getTable());
        if(previous == null) {
            return null;
        }

        // get previous schema to handle rollback
        String getSchema = "SHOW CREATE TABLE `" + table.getDatabase() + "`.`" + table.getTable() + "`";

        try {
            List<Map<String, Object>> result = hiveJdbcTemplate.queryForList(getSchema);
            return result.get(0).get("createtab_stmt").toString();
        }catch(Exception e){
            throw new HiveQueryException("Unable to get table schema: "+getSchema, e);
        }
    }

    public void deleteTable(HiveTable table) throws HiveQueryException {
        String dropSql = "DROP TABLE IF EXISTS `" + table.getDatabase() + "`.`" + table.getTable() + "`";
        log.info("Drop table: " + dropSql);

        try {
            long startTime = System.currentTimeMillis();
            hiveJdbcTemplate.execute(dropSql);
            log.info("SQL executed in: " + (System.currentTimeMillis() - startTime)+" ms");
        }catch(Exception e){
            throw new HiveQueryException("Unable to create table: "+dropSql, e);
        }
    }

    private String buildTableProperties(HiveTable table, HdfsService.HdfsFileInfo dataSetConfigfileInfo) {

        String jsonTags = "[]";
        try {
            jsonTags = objectMapper.writeValueAsString(table.getTags());
        } catch (JsonProcessingException e) {
            log.warn("Unable to serialize tags", e);
        }

        String result = "TBLPROPERTIES (" +
                "\"format\" = \""+table.getFormat()+"\"," +
                "\"creator\" = \""+table.getCreator()+"\"," +
                "\"comment\" = \""+table.getComment()+"\"," +
                "\"temporary\" = \""+table.getTemporary()+"\"," +
                "\"tags\" = \""+jsonTags.replace("\"", "\\\"")+"\"";

        if(table.getOtherProperties() != null) {
            for (Map.Entry<String, String> entry : table.getOtherProperties().entrySet()) {
                boolean reserved = false;
                for (String key : reservedKeys) {
                    if (key.equalsIgnoreCase(entry.getKey())) {
                        reserved = true;
                        break;
                    }
                }

                if (!reserved) {
                    result += ",\"" + entry.getKey() + "\" = \"" + entry.getValue() + "\"";
                }
            }
        }

        if(dataSetConfigfileInfo != null) {
            result += "," +
            "\"dataSetConfigPath\" = \""+dataSetConfigfileInfo.getFilePath()+"\"," +
            "\"dataSetType\" = \"" + table.getDataSetType() + "\"";
        }

        result += ")";
        return result;
    }

    private HiveTable buildHiveTable(Table table) throws TException {
        HiveTable hiveTable = new HiveTable();
        hiveTable.setDatabase(table.getDbName());
        hiveTable.setTable(table.getTableName());
        hiveTable.setFormat(table.getParameters().getOrDefault("format", ""));
        hiveTable.setCreator(table.getParameters().getOrDefault("creator", ""));
        hiveTable.setComment(table.getParameters().getOrDefault("comment", ""));

        String temporary = table.getParameters().getOrDefault("temporary", "false");
        if(Strings.isNullOrEmpty(temporary) || "null".equalsIgnoreCase(temporary)){
            hiveTable.setTemporary(false);
        }
        else {
            hiveTable.setTemporary(Boolean.parseBoolean(temporary));
        }

        String tagsJson = table.getParameters().getOrDefault("tags", "[]");
        String[] tags = new String[0];
        try {
            tags = objectMapper.readValue(tagsJson, String[].class);
        } catch (IOException e) {
            log.warn("Unable to parse tags for table "+table.getDbName()+"."+table.getTableName(), e);
        }
        hiveTable.setTags(tags);

        hiveTable.setDataSetType(table.getParameters().getOrDefault("dataSetType", DataSetType.PublicDataSet.name()));

        String dataSetConfigPath = table.getParameters().getOrDefault("dataSetConfigPath", "");
        if(!Strings.isNullOrEmpty(dataSetConfigPath)) {
            try {
                InputStream in = hdfsService.read(dataSetConfigPath);
                StringWriter writer = new StringWriter();
                IOUtils.copy(in, writer, StandardCharsets.UTF_8);
                hiveTable.setDataSetConfiguration(writer.toString());
            } catch (IOException e) {
                log.warn("Unable to read dataSetConfig for table "+table.getDbName()+"."+table.getTableName(), e);
            }
        }

        Map<String, String> otherProperties = new HashMap<>();

        for(Map.Entry<String, String> entry : table.getParameters().entrySet()) {
            boolean reserved = false;
            for(String key : reservedKeys){
                if(key.equalsIgnoreCase(entry.getKey())) {
                    reserved = true;
                    break;
                }
            }

            if(!reserved) {
                otherProperties.put(entry.getKey(), entry.getValue());
            }
        }
        hiveTable.setOtherProperties(otherProperties);

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
