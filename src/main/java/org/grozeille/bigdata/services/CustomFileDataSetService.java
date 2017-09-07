package org.grozeille.bigdata.services;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.execution.Except;
import org.apache.thrift.TException;
import org.grozeille.bigdata.resources.hive.model.HiveColumn;
import org.grozeille.bigdata.resources.hive.model.HiveColumnStatistics;
import org.grozeille.bigdata.resources.hive.model.HiveTable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.client.HttpClientErrorException;

import java.io.IOException;
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

    public void createFromCsv(String database, String table, String filename, InputStream inputStream, CsvOptions options) throws Exception {
        HiveTable hiveTable = hiveService.findOne(database, table);

        String originalFilePath = hdfsService.write(inputStream, filename, hiveTable.getPath());
        inputStream.close();
        InputStream in = hdfsService.read(originalFilePath);
        hiveTable.setOriginalFile(originalFilePath);

        String[] columns = this.csvParserService.write(
                in,
                options.getSeparator(),
                options.getTextQualifier(),
                options.isFirstLineHeader(),
                hiveTable.getPath());

        List<HiveColumn> hiveColumns = new ArrayList<>();
        for(String c : columns){
            hiveColumns.add(new HiveColumn(c, "STRING", "", new HiveColumnStatistics()));
        }
        hiveTable.setColumns(hiveColumns.toArray(new HiveColumn[0]));
        this.hiveService.createOrcTable(hiveTable);
    }

    public void createFromRaw(String database, String table, String filename, InputStream inputStream) throws Exception {
        HiveTable hiveTable = hiveService.findOne(database, table);

        String originalFilePath = hdfsService.write(inputStream, filename, hiveTable.getPath());
        inputStream.close();
        InputStream in = hdfsService.read(originalFilePath);
        hiveTable.setOriginalFile(originalFilePath);

        String[] columns = this.rawParserService.write(
                in,
                hiveTable.getPath());

        List<HiveColumn> hiveColumns = new ArrayList<>();
        for(String c : columns){
            hiveColumns.add(new HiveColumn(c, "STRING", "", new HiveColumnStatistics()));
        }
        hiveTable.setColumns(hiveColumns.toArray(new HiveColumn[0]));
        this.hiveService.createOrcTable(hiveTable);
    }

    public void createFromExcel(String database, String table, String filename, InputStream inputStream, ExcelOptions options) throws Exception {
        HiveTable hiveTable = hiveService.findOne(database, table);

        String originalFilePath = hdfsService.write(inputStream, filename, hiveTable.getPath());
        inputStream.close();
        InputStream in = hdfsService.read(originalFilePath);
        hiveTable.setOriginalFile(originalFilePath);

        String[] columns = this.excelParserService.write(
                in,
                filename,
                options.getSheet(),
                options.isFirstLineHeader(),
                hiveTable.getPath());

        List<HiveColumn> hiveColumns = new ArrayList<>();
        for(String c : columns){
            hiveColumns.add(new HiveColumn(c, "STRING", "", new HiveColumnStatistics()));
        }
        hiveTable.setColumns(hiveColumns.toArray(new HiveColumn[0]));
        this.hiveService.createOrcTable(hiveTable);
    }
}
