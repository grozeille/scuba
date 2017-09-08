package org.grozeille.bigdata.services;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.Path;
import org.apache.thrift.TException;
import org.grozeille.bigdata.resources.hive.model.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

@Service
@Slf4j
public class CustomFileDataSetService {

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class CsvOptions {
        private Character separator;
        private Character textQualifier;
        private boolean firstLineHeader;
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class ExcelOptions {
        private String sheet;
        private boolean firstLineHeader;
    }

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

    public void createOrUpdateOrcTable(String database, String table, String comment, String creator, String[] tags, Boolean temporary) throws Exception {
        HiveTable hiveTable = hiveService.findOne(database, table);

        if(hiveTable == null) {

            // create a temporary table
            hiveTable = new HiveTable();
            hiveTable.setDatabase(database);
            hiveTable.setTable(table);
            hiveTable.setComment(comment);
            hiveTable.setCreator(creator);
            hiveTable.setTags(tags);
            hiveTable.setTemporary(temporary);
            hiveTable.setColumns(new HiveColumn[]{new HiveColumn("line", "binary", "", new HiveColumnStatistics())});

            HiveDatabase hiveDatabase = hiveService.findOneDatabase(database);
            String tablePath = hiveDatabase.getPath() + "/" + table;
            hiveTable.setPath(tablePath);

            hiveService.createOrcTable(hiveTable);
        }
        else {
            hiveTable.setComment(comment);
            hiveTable.setCreator(creator);
            hiveTable.setTags(tags);
            hiveTable.setTemporary(temporary);

            hiveService.update(hiveTable);
        }
        if(!temporary) {
            dataSetService.refreshTable(database, table);
        }
    }

    public void uploadFile(String database, String table, String filename, String contentType, InputStream inputStream) throws Exception {
        HiveTable hiveTable = hiveService.findOne(database, table);

        HdfsService.HdfsFileInfo originalFile = hdfsService.write(inputStream, filename, hiveTable.getPath());
        inputStream.close();
        hiveTable.setOriginalFile(originalFile.getFilePath());
        hiveTable.setOriginalFileSize(originalFile.getSize());
        hiveTable.setOriginalFileContentType(contentType);

        hiveService.update(hiveTable);
        dataSetService.refreshTable(database, table);
    }

    public HiveData parseRawFile(String database, String table, Long maxLinePreview) throws Exception {
        HiveTable hiveTable = hiveService.findOne(database, table);

        try(InputStream in = hdfsService.read(hiveTable.getOriginalFile())) {
            return this.rawParserService.data(in, maxLinePreview);
        }
    }

    public void createFromRaw(String database, String table) throws Exception {
        HiveTable hiveTable = hiveService.findOne(database, table);

        try(InputStream in = hdfsService.read(hiveTable.getOriginalFile())) {

            hiveTable.setFormat("RAW");

            String[] columns = this.rawParserService.write(
                    in,
                    hiveTable.getPath());

            List<HiveColumn> hiveColumns = new ArrayList<>();
            for (String c : columns) {
                hiveColumns.add(new HiveColumn(c, "STRING", "", new HiveColumnStatistics()));
            }
            hiveTable.setColumns(hiveColumns.toArray(new HiveColumn[0]));
            this.hiveService.createOrcTable(hiveTable);
            dataSetService.refreshTable(database, table);
        }
    }

    public HiveData parseCsvFile(String database, String table, CsvOptions options, Long maxLinePreview) throws Exception {
        HiveTable hiveTable = hiveService.findOne(database, table);

        try(InputStream in = hdfsService.read(hiveTable.getOriginalFile())) {

            return this.csvParserService.data(
                    in,
                    options.getSeparator(),
                    options.getTextQualifier(),
                    options.isFirstLineHeader(),
                    maxLinePreview);
        }
    }

    public void createFromCsv(String database, String table, CsvOptions options) throws Exception {
        HiveTable hiveTable = hiveService.findOne(database, table);

        try(InputStream in = hdfsService.read(hiveTable.getOriginalFile())) {

            hiveTable.setFormat("CSV");

            String[] columns = this.csvParserService.write(
                    in,
                    options.getSeparator(),
                    options.getTextQualifier(),
                    options.isFirstLineHeader(),
                    hiveTable.getPath());

            List<HiveColumn> hiveColumns = new ArrayList<>();
            for (String c : columns) {
                hiveColumns.add(new HiveColumn(c, "STRING", "", new HiveColumnStatistics()));
            }
            hiveTable.setColumns(hiveColumns.toArray(new HiveColumn[0]));
            this.hiveService.createOrcTable(hiveTable);
            dataSetService.refreshTable(database, table);
        }
    }

    public HiveData parseExcelFile(String database, String table, ExcelOptions options, Long maxLinePreview) throws Exception {
        HiveTable hiveTable = hiveService.findOne(database, table);

        try(InputStream in = hdfsService.read(hiveTable.getOriginalFile())) {

            return this.excelParserService.data(
                    in,
                    new Path(hiveTable.getOriginalFile()).getName(),
                    options.getSheet(),
                    options.isFirstLineHeader(),
                    maxLinePreview);
        }
    }

    public String[] sheets(String database, String table) throws Exception {
        HiveTable hiveTable = hiveService.findOne(database, table);

        try(InputStream in = hdfsService.read(hiveTable.getOriginalFile())) {

            return this.excelParserService.sheets(
                    in,
                    new Path(hiveTable.getOriginalFile()).getName());
        }
    }

    public void createFromExcel(String database, String table, ExcelOptions options) throws Exception {
        HiveTable hiveTable = hiveService.findOne(database, table);

        try(InputStream in = hdfsService.read(hiveTable.getOriginalFile())) {

            hiveTable.setFormat("EXCEL");

            String[] columns = this.excelParserService.write(
                    in,
                    new Path(hiveTable.getOriginalFile()).getName(),
                    options.getSheet(),
                    options.isFirstLineHeader(),
                    hiveTable.getPath());

            List<HiveColumn> hiveColumns = new ArrayList<>();
            for (String c : columns) {
                hiveColumns.add(new HiveColumn(c, "STRING", "", new HiveColumnStatistics()));
            }
            hiveTable.setColumns(hiveColumns.toArray(new HiveColumn[0]));
            this.hiveService.createOrcTable(hiveTable);
            dataSetService.refreshTable(database, table);
        }
    }

    public InputStream downloadFile(String database, String table) throws Exception {
        HiveTable hiveTable = hiveService.findOne(database, table);
        return hdfsService.read(hiveTable.getOriginalFile());
    }
}
