package org.grozeille.bigdata.dataset.services;


import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.Path;
import org.apache.thrift.TException;
import org.grozeille.bigdata.dataset.exceptions.HiveDatabaseNotFoundException;
import org.grozeille.bigdata.dataset.exceptions.HiveTableAlreadyExistsException;
import org.grozeille.bigdata.dataset.exceptions.HiveTableNotFoundException;
import org.grozeille.bigdata.dataset.model.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

@Service
@Slf4j
public class CustomFileDataSetService {

    @Autowired
    private ExcelParserService excelParserService;

    @Autowired
    private CsvParserService csvParserService;

    @Autowired
    private RawParserService rawParserService;

    @Autowired
    private HiveService hiveService;

    @Autowired
    private HdfsService hdfsService;

    @Autowired
    private DataSetService dataSetService;

    private final ObjectMapper objectMapper = new ObjectMapper();

    public void createOrUpdateCustomFileTable(
            DataSetConf dataSetConf,
            String creator,
            Boolean temporary,
            CustomFileDataSetConf customFileDataSetConf) throws Exception {


        HiveDatabase hiveDatabase = hiveService.findOneDatabase(dataSetConf.getDatabase());
        if(hiveDatabase == null) {
            throw new HiveDatabaseNotFoundException(dataSetConf.getDatabase() + " not found");
        }

        String tableName = dataSetConf.getTable();

        HiveTable hiveTable = hiveService.findOne(dataSetConf.getDatabase(), tableName);

        if(hiveTable == null) {
            String dataSetType = DataSetType.CustomFileDataSet.name();
            CustomFileDataSetConf customFileDataSetConfOriginal = new CustomFileDataSetConf();
            customFileDataSetConfOriginal.setFileFormat(customFileDataSetConf.getFileFormat());
            customFileDataSetConfOriginal.setFirstLineHeader(customFileDataSetConf.isFirstLineHeader());
            customFileDataSetConfOriginal.setSeparator(customFileDataSetConf.getSeparator());
            customFileDataSetConfOriginal.setTextQualifier(customFileDataSetConf.getTextQualifier());
            customFileDataSetConfOriginal.setSheet(customFileDataSetConf.getSheet());

            String json = objectMapper.writeValueAsString(customFileDataSetConfOriginal);

            hiveTable = new HiveTable();
            hiveTable.setDatabase(dataSetConf.getDatabase());
            hiveTable.setTable(dataSetConf.getTable());
            hiveTable.setComment(dataSetConf.getComment());
            hiveTable.setTags(dataSetConf.getTags());
            hiveTable.setCreator(creator);
            hiveTable.setTemporary(temporary);
            hiveTable.setDataSetType(dataSetType);
            hiveTable.setDataSetConfiguration(json);
            hiveTable.setColumns(new HiveColumn[]{new HiveColumn("line", "binary", "", new HiveColumnStatistics())});

            String tablePath = hiveDatabase.getPath() + "/" + dataSetConf.getTable();
            hiveTable.setPath(tablePath);

            hiveService.createOrcTable(hiveTable);
        }
        else {

            CustomFileDataSetConf customFileDataSetConfOriginal;

            if(hiveTable.getDataSetConfiguration() != null) {
                customFileDataSetConfOriginal = objectMapper.readValue(hiveTable.getDataSetConfiguration(), CustomFileDataSetConf.class);
            }
            else {
                customFileDataSetConfOriginal = new CustomFileDataSetConf();
            }

            customFileDataSetConfOriginal.setFileFormat(customFileDataSetConf.getFileFormat());
            customFileDataSetConfOriginal.setFirstLineHeader(customFileDataSetConf.isFirstLineHeader());
            customFileDataSetConfOriginal.setSeparator(customFileDataSetConf.getSeparator());
            customFileDataSetConfOriginal.setTextQualifier(customFileDataSetConf.getTextQualifier());
            customFileDataSetConfOriginal.setSheet(customFileDataSetConf.getSheet());

            if(customFileDataSetConf.getOriginalFile() != null) {
                customFileDataSetConfOriginal.setOriginalFile(customFileDataSetConf.getOriginalFile());
            }

            String json = objectMapper.writeValueAsString(customFileDataSetConfOriginal);

            hiveTable.setComment(dataSetConf.getComment());
            hiveTable.setTags(dataSetConf.getTags());
            hiveTable.setCreator(creator);
            hiveTable.setDataSetConfiguration(json);
            hiveTable.setTemporary(temporary);

            hiveService.updateTable(hiveTable);
        }

        dataSetService.refreshTable(dataSetConf.getDatabase(), dataSetConf.getTable());
    }

    public void uploadFile(
            String database,
            String table,
            String filename,
            String contentType,
            InputStream inputStream) throws Exception {

        HiveTable hiveTable = hiveService.findOne(database, table);

        if(hiveTable == null) {
            throw new HiveTableNotFoundException(database + "." + table + " not found");
        }

        CustomFileDataSetConf config = extractDataSetConf(hiveTable);

        HdfsService.HdfsFileInfo originalFile = hdfsService.write(inputStream, filename, hiveTable.getPath());
        inputStream.close();
        config.setOriginalFile(new CustomFileDataSetFileInfo());
        config.getOriginalFile().setPath(originalFile.getFilePath());
        config.getOriginalFile().setSize(originalFile.getSize());
        config.getOriginalFile().setContentType(contentType);

        hiveTable.setDataSetConfiguration(objectMapper.writeValueAsString(config));

        hiveService.updateTable(hiveTable);

        dataSetService.refreshTable(database, table);
    }

    public CustomFileDataSetConf extractDataSetConf(HiveTable hiveTable) throws java.io.IOException {
        return objectMapper.readValue(hiveTable.getDataSetConfiguration(), CustomFileDataSetConf.class);
    }

    public void updateTableSchema(String database, String table) throws Exception {

        HiveTable hiveTable = hiveService.findOne(database, table);

        if(hiveTable == null) {
            throw new HiveTableNotFoundException(database + "." + table + " not found");
        }

        CustomFileDataSetConf config = extractDataSetConf(hiveTable);

        if(config.getFileFormat() == CustomFileDataSetConf.CustomFileDataSetFormat.RAW) {
            updateTableSchemaFromRaw(hiveTable, config);
        }
        else if(config.getFileFormat() == CustomFileDataSetConf.CustomFileDataSetFormat.CSV) {
            updateTableSchemaFromCsv(hiveTable, config);
        }
        else if(config.getFileFormat() == CustomFileDataSetConf.CustomFileDataSetFormat.EXCEL) {
            updateTableSchemaFromExcel(hiveTable, config);
        }
    }

    private void updateTableSchemaFromRaw(HiveTable hiveTable, CustomFileDataSetConf config) throws Exception {
        if(config.getOriginalFile() == null || config.getOriginalFile().getPath() == null) {
            log.warn("No original file for the hive table " + hiveTable.getDatabase() + "." + hiveTable.getTable() + ", update schema skipped");
        }

        try(InputStream in = hdfsService.read(config.getOriginalFile().getPath())) {

            hiveTable.setFormat("RAW");

            String[] columns = this.rawParserService.write(
                    in,
                    hiveTable.getPath());

            List<HiveColumn> hiveColumns = new ArrayList<>();
            for (String c : columns) {
                hiveColumns.add(new HiveColumn(c, "STRING", "", new HiveColumnStatistics()));
            }
            hiveTable.setColumns(hiveColumns.toArray(new HiveColumn[0]));

            hiveService.createOrcTable(hiveTable);

            dataSetService.refreshTable(hiveTable.getDatabase(), hiveTable.getTable());
        }
    }

    private void updateTableSchemaFromCsv(HiveTable hiveTable, CustomFileDataSetConf config) throws Exception {
        try(InputStream in = hdfsService.read(config.getOriginalFile().getPath())) {

            hiveTable.setFormat("CSV");

            String[] columns = this.csvParserService.write(
                    in,
                    config.getSeparator(),
                    config.getTextQualifier(),
                    config.isFirstLineHeader(),
                    hiveTable.getPath());

            List<HiveColumn> hiveColumns = new ArrayList<>();
            for (String c : columns) {
                hiveColumns.add(new HiveColumn(c, "STRING", "", new HiveColumnStatistics()));
            }
            hiveTable.setColumns(hiveColumns.toArray(new HiveColumn[0]));

            hiveService.createOrcTable(hiveTable);

            dataSetService.refreshTable(hiveTable.getDatabase(), hiveTable.getTable());
        }
    }

    public String[] sheets(String database, String table) throws Exception {
        HiveTable hiveTable = hiveService.findOne(database, table);

        CustomFileDataSetConf config = extractDataSetConf(hiveTable);

        try(InputStream in = hdfsService.read(config.getOriginalFile().getPath())) {

            return this.excelParserService.sheets(
                    in,
                    new Path(config.getOriginalFile().getPath()).getName());
        }
    }

    private void updateTableSchemaFromExcel(HiveTable hiveTable, CustomFileDataSetConf config) throws Exception {
        try(InputStream in = hdfsService.read(config.getOriginalFile().getPath())) {

            hiveTable.setFormat("EXCEL");

            String[] columns = this.excelParserService.write(
                    in,
                    new Path(config.getOriginalFile().getPath()).getName(),
                    config.getSheet(),
                    config.isFirstLineHeader(),
                    hiveTable.getPath());

            List<HiveColumn> hiveColumns = new ArrayList<>();
            for (String c : columns) {
                hiveColumns.add(new HiveColumn(c, "STRING", "", new HiveColumnStatistics()));
            }
            hiveTable.setColumns(hiveColumns.toArray(new HiveColumn[0]));

            hiveService.createOrcTable(hiveTable);

            dataSetService.refreshTable(hiveTable.getDatabase(), hiveTable.getTable());
        }
    }

    public CustomFileDataSetFileStream downloadFile(String database, String table) throws Exception {
        HiveTable hiveTable = hiveService.findOne(database, table);

        CustomFileDataSetConf config = extractDataSetConf(hiveTable);

        CustomFileDataSetFileStream fileStream = new CustomFileDataSetFileStream();
        fileStream.setFileInfo(config.getOriginalFile());
        fileStream.setInputStream(hdfsService.read(config.getOriginalFile().getPath()));

        return fileStream;
    }

    public void clone(
            String sourceDatabase,
            String sourceTable,
            String targetDatabase,
            String targetTable,
            String creator,
            Boolean temporary) throws Exception {
        HiveTable sourceHiveTable = hiveService.findOne(sourceDatabase, sourceTable);

        if(sourceHiveTable == null) {
            throw new HiveTableNotFoundException(sourceDatabase + "." + sourceTable + " not found");
        }

        HiveTable targetHiveTable = hiveService.findOne(targetDatabase, targetTable);

        if(targetHiveTable != null && targetHiveTable.getTemporary() == false) {
            throw new HiveTableAlreadyExistsException(targetDatabase + "." + targetTable + " already exists");
        }

        // create the table
        this.createOrUpdateCustomFileTable(
                new DataSetConf(targetDatabase, targetTable, sourceHiveTable.getComment(), sourceHiveTable.getTags()),
                creator,
                temporary,
                new CustomFileDataSetConf()
        );

        // clone the file
        CustomFileDataSetConf customFileDataSetConf = objectMapper.readValue(sourceHiveTable.getDataSetConfiguration(), CustomFileDataSetConf.class);

        HdfsService.HdfsFileInfo hdfsFileInfo = hdfsService.copy(customFileDataSetConf.getOriginalFile().getPath(), targetTable);
        customFileDataSetConf.getOriginalFile().setPath(hdfsFileInfo.getFilePath());

        // update the table
        this.createOrUpdateCustomFileTable(
                new DataSetConf(targetDatabase, targetTable, sourceHiveTable.getComment(), sourceHiveTable.getTags()),
                creator,
                temporary,
                customFileDataSetConf
        );

        // update the schema
        this.updateTableSchema(targetDatabase, targetTable);

    }
}
