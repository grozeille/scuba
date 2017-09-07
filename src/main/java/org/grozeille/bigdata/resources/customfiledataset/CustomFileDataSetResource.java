package org.grozeille.bigdata.resources.customfiledataset;

import lombok.extern.slf4j.Slf4j;
import org.grozeille.bigdata.resources.hive.model.HiveData;
import org.grozeille.bigdata.resources.hive.model.HiveTable;
import org.grozeille.bigdata.services.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.multipart.MultipartFile;

@RestController
@Slf4j
@RequestMapping("/api/dataset")
public class CustomFileDataSetResource {

    private static final Long MAX_LINES_PREVIEW = 5000l;

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

    @Autowired
    private CustomFileDataSetService customFileDataSetService;

    @RequestMapping(value = "/custom-file/preview/data/raw", method = RequestMethod.POST)
    public HiveData data(@RequestParam("file") MultipartFile file) throws Exception {

        return this.rawParserService.data(file.getInputStream(), MAX_LINES_PREVIEW);
    }

    @RequestMapping(value = "/custom-file/preview/data/csv", method = RequestMethod.POST)
    public HiveData data(@RequestParam("file") MultipartFile file,
                         @RequestParam(value = "separator", required = true) Character separator,
                         @RequestParam(value = "textQualifier", required = true, defaultValue = "") Character textQualifier,
                         @RequestParam(value = "firstLineHeader", required = true, defaultValue = "false") boolean firstLineHeader) throws Exception {

        return this.csvParserService.data(file.getInputStream(), separator, textQualifier, firstLineHeader, MAX_LINES_PREVIEW);
    }

    @RequestMapping(value = "/custom-file/preview/data/excel/sheets", method = RequestMethod.POST)
    public String[] sheets(@RequestParam("file") MultipartFile file) throws Exception {
        return this.excelParserService.sheets(file.getInputStream(), file.getOriginalFilename());
    }

    @RequestMapping(value = "/custom-file/preview/data/excel", method = RequestMethod.POST)
    public HiveData data(@RequestParam("file") MultipartFile file,
                         @RequestParam(value = "sheet", required = true) String sheet,
                         @RequestParam(value = "firstLineHeader", required = true, defaultValue = "false") boolean firstLineHeader) throws Exception {

        return this.excelParserService.data(file.getInputStream(), file.getOriginalFilename(), sheet, firstLineHeader, MAX_LINES_PREVIEW);
    }

    @RequestMapping(value = "/custom-file/{database}/{table}/data/raw", method = RequestMethod.POST, produces = MediaType.APPLICATION_JSON_VALUE, consumes = "multipart/mixed")
    @Transactional(readOnly = false)
    public void createFromRaw(
            @PathVariable("database") String database,
            @PathVariable("table") String table,
            @RequestPart("file") MultipartFile file) throws Exception {

        // check if exists
        HiveTable hiveTable = hiveService.findOne(database, table);
        if(hiveTable == null){
            throw new HttpClientErrorException(HttpStatus.BAD_REQUEST, "Table '"+database+"."+table+"' does not exist");
        }

        customFileDataSetService.createFromRaw(
                database,
                table,
                file.getOriginalFilename(),
                file.getInputStream());
    }

    @RequestMapping(value = "/custom-file/{database}/{table}/data/csv", method = RequestMethod.POST)
    @Transactional(readOnly = false)
    public void createFromCsv(
            @PathVariable("database") String database,
            @PathVariable("table") String table,
            @RequestPart("file") MultipartFile file,
            @RequestParam(value = "separator", required = true) Character separator,
            @RequestParam(value = "textQualifier", required = true, defaultValue = "") Character textQualifier,
            @RequestParam(value = "firstLineHeader", required = true, defaultValue = "false") boolean firstLineHeader) throws Exception {

        // check if exists
        HiveTable hiveTable = hiveService.findOne(database, table);
        if(hiveTable == null){
            throw new HttpClientErrorException(HttpStatus.BAD_REQUEST, "Table '"+database+"."+table+"' does not exist");
        }

        customFileDataSetService.createFromCsv(
                database,
                table,
                file.getOriginalFilename(),
                file.getInputStream(),
                new CustomFileDataSetService.CsvOptions(
                        separator,
                        textQualifier,
                        firstLineHeader
                )
        );

    }

    @RequestMapping(value = "/custom-file/{database}/{table}/data/excel", method = RequestMethod.POST)
    @Transactional(readOnly = false)
    public void createFromExcel(
            @PathVariable("database") String database,
            @PathVariable("table") String table,
            @RequestPart("file") MultipartFile file,
            @RequestParam(value = "sheet", required = true) String sheet,
            @RequestParam(value = "firstLineHeader", required = true, defaultValue = "false") boolean firstLineHeader) throws Exception {

        // check if exists
        HiveTable hiveTable = hiveService.findOne(database, table);
        if(hiveTable == null){
            throw new HttpClientErrorException(HttpStatus.BAD_REQUEST, "Table '"+database+"."+table+"' does not exist");
        }

        customFileDataSetService.createFromExcel(
                database,
                table,
                file.getOriginalFilename(),
                file.getInputStream(),
                new CustomFileDataSetService.ExcelOptions(
                        sheet,
                        firstLineHeader
                )
        );

    }

}
