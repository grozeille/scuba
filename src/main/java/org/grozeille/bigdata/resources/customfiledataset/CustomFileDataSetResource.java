package org.grozeille.bigdata.resources.customfiledataset;

import io.swagger.annotations.ApiParam;
import lombok.extern.slf4j.Slf4j;
import org.apache.thrift.TException;
import org.grozeille.bigdata.resources.customfiledataset.model.CustomFileDataSetFormatRequest;
import org.grozeille.bigdata.resources.customfiledataset.model.CustomFileDataSetRequest;
import org.grozeille.bigdata.resources.customfiledataset.model.PreviewCustomFileDataSetFormatRequest;
import org.grozeille.bigdata.resources.hive.model.*;
import org.grozeille.bigdata.services.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.multipart.MultipartFile;
import springfox.documentation.annotations.ApiIgnore;

import java.security.Principal;

@RestController
@Slf4j
@RequestMapping("/api/dataset")
public class CustomFileDataSetResource {

    @Autowired
    private HiveService hiveService;

    @Autowired
    private DataSetService dataSetService;

    @Autowired
    private CustomFileDataSetService customFileDataSetService;

    @RequestMapping(value = "/custom-file/{database}/{table}/file", method = RequestMethod.GET)
    public HiveData data(
            @PathVariable("database") String database,
            @PathVariable("table") String table,
            @RequestBody PreviewCustomFileDataSetFormatRequest request) throws Exception {

        checkIfExists(database, table);

        if(request.getFormat() == PreviewCustomFileDataSetFormatRequest.CustomFileDataSetFormat.RAW) {
            return customFileDataSetService.parseRawFile(
                    database,
                    table,
                    request.getMaxLinePreview());
        }
        else if(request.getFormat() == PreviewCustomFileDataSetFormatRequest.CustomFileDataSetFormat.CSV) {

            if(request.getSeparator() == null) {
                throw new HttpClientErrorException(HttpStatus.BAD_REQUEST, "separator is required for format csv");
            }
            else if(request.getTextQualifier() == null){
                throw new HttpClientErrorException(HttpStatus.BAD_REQUEST, "textQualifier is required for format csv");
            }

            return customFileDataSetService.parseCsvFile(
                    database,
                    table,
                    new CustomFileDataSetService.CsvOptions(
                            request.getSeparator(),
                            request.getTextQualifier(),
                            request.isFirstLineHeader()
                    ),
                    request.getMaxLinePreview()
            );
        }
        else if(request.getFormat() == PreviewCustomFileDataSetFormatRequest.CustomFileDataSetFormat.EXCEL) {
            if(request.getSheet() == null) {
                throw new HttpClientErrorException(HttpStatus.BAD_REQUEST, "sheet is required for format excel");
            }

            return customFileDataSetService.parseExcelFile(
                    database,
                    table,
                    new CustomFileDataSetService.ExcelOptions(
                            request.getSheet(),
                            request.isFirstLineHeader()
                    ),
                    request.getMaxLinePreview()
            );
        }
        else {
            throw new HttpClientErrorException(HttpStatus.BAD_REQUEST, "format '"+request.getFormat()+"' is invalid");
        }
    }


    @RequestMapping(value = "/custom-file/{database}/{table}/file/sheets", method = RequestMethod.POST)
    public String[] sheets(@PathVariable("database") String database,
                           @PathVariable("table") String table) throws Exception {
        return this.customFileDataSetService.sheets(database, table);
    }

    @RequestMapping(value = "/custom-file/{database}/{table}/file", method = RequestMethod.PUT)
    @Transactional(readOnly = false)
    public void uploadFile(
            @PathVariable("database") String database,
            @PathVariable("table") String table,
            @RequestPart("file") MultipartFile file) throws Exception {

        checkIfExists(database, table);

        customFileDataSetService.uploadFile(database, table, file.getOriginalFilename(), file.getInputStream());
    }

    @RequestMapping(value = "/custom-file/{database}/{table}", method = RequestMethod.PUT)
    public void createCustomFileDataSet(
            @ApiIgnore @ApiParam(hidden = true) Principal principal,
            @PathVariable("database") String database,
            @PathVariable("table") String table,
            @RequestBody CustomFileDataSetRequest createCustomFileDataSetRequest) throws Exception {

        this.customFileDataSetService.createOrUpdateTemporaryTable(
                database,
                table,
                createCustomFileDataSetRequest.getComment(),
                principal.getName(),
                createCustomFileDataSetRequest.getTags()
        );
    }

    @RequestMapping(value = "/custom-file/{database}/{table}/update", method = RequestMethod.POST)
    @Transactional(readOnly = false)
    public void updateSchema(
            @PathVariable("database") String database,
            @PathVariable("table") String table,
            @RequestBody CustomFileDataSetFormatRequest request) throws Exception {

        checkIfExists(database, table);

        if(request.getFormat() == CustomFileDataSetFormatRequest.CustomFileDataSetFormat.RAW) {
            customFileDataSetService.createFromRaw(
                    database,
                    table);
        }
        else if(request.getFormat() == CustomFileDataSetFormatRequest.CustomFileDataSetFormat.CSV) {

            if(request.getSeparator() == null) {
                throw new HttpClientErrorException(HttpStatus.BAD_REQUEST, "separator is required for format csv");
            }
            else if(request.getTextQualifier() == null){
                throw new HttpClientErrorException(HttpStatus.BAD_REQUEST, "textQualifier is required for format csv");
            }

            customFileDataSetService.createFromCsv(
                    database,
                    table,
                    new CustomFileDataSetService.CsvOptions(
                            request.getSeparator() == null ? null : request.getSeparator().charAt(0),
                            request.getTextQualifier() == null ? null : request.getTextQualifier().charAt(0),
                            request.isFirstLineHeader()
                    )
            );
        }
        else if(request.getFormat() == CustomFileDataSetFormatRequest.CustomFileDataSetFormat.EXCEL) {
            if(request.getSheet() == null) {
                throw new HttpClientErrorException(HttpStatus.BAD_REQUEST, "sheet is required for format excel");
            }

            customFileDataSetService.createFromExcel(
                    database,
                    table,
                    new CustomFileDataSetService.ExcelOptions(
                            request.getSheet(),
                            request.isFirstLineHeader()
                    )
            );
        }
        else {
            throw new HttpClientErrorException(HttpStatus.BAD_REQUEST, "format '"+request.getFormat()+"' is invalid");
        }
    }

    private void checkIfExists(@PathVariable("database") String database, @PathVariable("table") String table) throws TException {
        HiveTable hiveTable = hiveService.findOne(database, table);
        if(hiveTable == null){
            throw new HttpClientErrorException(HttpStatus.BAD_REQUEST, "Table '"+database+"."+table+"' does not exist");
        }
        else if(!HiveService.DATALAKE_ITEM_TYPE_FILE_DATA_SET.equals(hiveTable.getDatalakeItemType())) {
            // test if the table is a customFileDataSet
            throw new HttpClientErrorException(HttpStatus.BAD_REQUEST, "Table '"+database+"."+table+"' is not a custom file dataset");
        }
    }

}
