package org.grozeille.bigdata.resources.hive;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.grozeille.bigdata.resources.userdataset.model.UserDataSetConf;
import org.grozeille.bigdata.resources.hive.model.*;
import org.grozeille.bigdata.services.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.thrift.TException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.multipart.MultipartFile;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;


@RestController
@Slf4j
@RequestMapping("/api/hive")
public class HiveResource {

    @Autowired
    private HiveService hiveService;

    @Autowired
    private ExcelParserService excelParserService;

    @Autowired
    private CsvParserService csvParserService;

    @Autowired
    private RawParserService rawParserService;

    @Autowired
    private HdfsService hdfsService;

    private static final Long MAX_LINES_PREVIEW = 5000l;

    @RequestMapping(value = "/tables", method = RequestMethod.GET)
    public HiveTable[] tables() throws TException {
        return hiveService.findAllPublicTables().toArray(HiveTable[]::new);
    }

    @RequestMapping(value = "/tables/{database}/{table}", method = RequestMethod.GET)
    public HiveTable table(@PathVariable("database") String database, @PathVariable("table") String table) throws TException {
        return hiveService.findOne(database, table);
    }

    @RequestMapping(value = "/data/dataset", method = RequestMethod.POST)
    public HiveData data(
            @RequestBody UserDataSetConf userDataSetConf,
            @RequestParam(value = "max", required = false, defaultValue = "10000") long max) throws Exception {

        HiveData hiveData = new HiveData();

        try {
            hiveData.setData(hiveService.getData(userDataSetConf, max));
        } catch (HiveInvalidDataSetException e) {
            throw e;
        } catch (Exception e) {
            log.error("Unexpected error", e);
            throw new Exception("Unexpected error");
        }

        return hiveData;
    }

    @RequestMapping(value = "/data/excel/sheets", method = RequestMethod.POST)
    public String[] sheets(@RequestParam("file") MultipartFile file) throws Exception {
        return this.excelParserService.sheets(file.getInputStream(), file.getOriginalFilename());
    }

    @RequestMapping(value = "/data/excel", method = RequestMethod.POST)
    public HiveData data(@RequestParam("file") MultipartFile file,
                         @RequestParam(value = "sheet", required = true) String sheet,
                         @RequestParam(value = "firstLineHeader", required = true, defaultValue = "false") boolean firstLineHeader) throws Exception {

        return this.excelParserService.data(file.getInputStream(), file.getOriginalFilename(), sheet, firstLineHeader, MAX_LINES_PREVIEW);
    }

    @RequestMapping(value = "/data/csv", method = RequestMethod.POST)
    public HiveData data(@RequestParam("file") MultipartFile file,
                         @RequestParam(value = "separator", required = true) Character separator,
                         @RequestParam(value = "textQualifier", required = true, defaultValue = "") Character textQualifier,
                         @RequestParam(value = "firstLineHeader", required = true, defaultValue = "false") boolean firstLineHeader) throws Exception {

        return this.csvParserService.data(file.getInputStream(), separator, textQualifier, firstLineHeader, MAX_LINES_PREVIEW);
    }

    @RequestMapping(value = "/data/raw", method = RequestMethod.POST)
    public HiveData data(@RequestParam("file") MultipartFile file) throws Exception {

        return this.rawParserService.data(file.getInputStream(), MAX_LINES_PREVIEW);
    }

    @RequestMapping(value = "/tables/{database}/{table}", method = RequestMethod.PUT)
    @Transactional(readOnly = false)
    public void save(
            @PathVariable("database") String database,
            @PathVariable("table") String table,
            @RequestBody HiveTableCreationRequest creationRequest) throws Exception {

        // check if exists
        HiveDatabase hiveDB = hiveService.findOneDatabase(database);
        if(hiveDB == null){
            throw new HttpClientErrorException(HttpStatus.BAD_REQUEST, "Database '"+database+"' does not exist");
        }

        HiveColumn hiveDummyColumn = new HiveColumn();
        hiveDummyColumn.setName("dummy");
        hiveDummyColumn.setType("STRING");

        HiveTable hiveTable = new HiveTable();
        hiveTable.setDatabase(database);
        hiveTable.setTable(table);
        hiveTable.setFormat("orc");
        hiveTable.setPath(hiveDB.getPath()+"/"+table);
        hiveTable.setDataDomainOwner(creationRequest.getDataDomainOwner());
        hiveTable.setComment(creationRequest.getComment());
        hiveTable.setTags(creationRequest.getTags());
        hiveTable.setColumns(new HiveColumn[]{ hiveDummyColumn });

        this.hiveService.createOrcTable(hiveTable);
    }

    @RequestMapping(value = "/tables/{database}/{table}/data/csv", method = RequestMethod.POST)
    @Transactional(readOnly = false)
    public void uploadCsv(
            @PathVariable("database") String database,
            @PathVariable("table") String table,
            @RequestPart("file") MultipartFile file,
            @RequestParam(required = true) Character separator,
            @RequestParam(required = true) Character textQualifier,
            @RequestParam boolean firstLineHeader) throws Exception {

        // check if exists
        HiveTable hiveTable = hiveService.findOne(database, table);
        if(hiveTable == null){
            throw new HttpClientErrorException(HttpStatus.BAD_REQUEST, "Table '"+database+"."+table+"' does not exist");
        }

        String originalFilePath = hdfsService.write(file.getInputStream(), file.getOriginalFilename(), hiveTable.getPath());
        file.getInputStream().close();
        InputStream in = hdfsService.read(originalFilePath);
        hiveTable.setOriginalFile(originalFilePath);

        String[] columns = this.csvParserService.write(
                in,
                separator,
                textQualifier,
                firstLineHeader,
                hiveTable.getPath());

        List<HiveColumn> hiveColumns = new ArrayList<>();
        for(String c : columns){
            hiveColumns.add(new HiveColumn(c, "STRING", "", new HiveColumnStatistics()));
        }
        hiveTable.setColumns(hiveColumns.toArray(new HiveColumn[0]));
        this.hiveService.createOrcTable(hiveTable);

    }

    @RequestMapping(value = "/tables/{database}/{table}/data/excel", method = RequestMethod.POST)
    @Transactional(readOnly = false)
    public void uploadExcel(
            @PathVariable("database") String database,
            @PathVariable("table") String table,
            @RequestPart("file") MultipartFile file,
            @RequestParam String sheet,
            @RequestParam boolean firstLineHeader) throws Exception {

        // check if exists
        HiveTable hiveTable = hiveService.findOne(database, table);
        if(hiveTable == null){
            throw new HttpClientErrorException(HttpStatus.BAD_REQUEST, "Table '"+database+"."+table+"' does not exist");
        }

        String originalFilePath = hdfsService.write(file.getInputStream(), file.getOriginalFilename(), hiveTable.getPath());
        file.getInputStream().close();
        InputStream in = hdfsService.read(originalFilePath);
        hiveTable.setOriginalFile(originalFilePath);

        String[] columns = this.excelParserService.write(
                in,
                file.getOriginalFilename(),
                sheet,
                firstLineHeader,
                hiveTable.getPath());

        List<HiveColumn> hiveColumns = new ArrayList<>();
        for(String c : columns){
            hiveColumns.add(new HiveColumn(c, "STRING", "", new HiveColumnStatistics()));
        }
        hiveTable.setColumns(hiveColumns.toArray(new HiveColumn[0]));
        this.hiveService.createOrcTable(hiveTable);

    }

    @RequestMapping(value = "/tables/{database}/{table}/data/raw", method = RequestMethod.POST)
    @Transactional(readOnly = false)
    public void uploadRaw(
            @PathVariable("database") String database,
            @PathVariable("table") String table,
            @RequestPart("file") MultipartFile file) throws Exception {

        // check if exists
        HiveTable hiveTable = hiveService.findOne(database, table);
        if(hiveTable == null){
            throw new HttpClientErrorException(HttpStatus.BAD_REQUEST, "Table '"+database+"."+table+"' does not exist");
        }

        String originalFilePath = hdfsService.write(file.getInputStream(), file.getOriginalFilename(), hiveTable.getPath());
        file.getInputStream().close();
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
}
