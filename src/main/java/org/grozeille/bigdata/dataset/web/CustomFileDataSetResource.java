package org.grozeille.bigdata.dataset.web;

import io.swagger.annotations.ApiParam;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.Path;
import org.apache.thrift.TException;
import org.grozeille.bigdata.dataset.model.*;
import org.grozeille.bigdata.dataset.services.CustomFileDataSetService;
import org.grozeille.bigdata.dataset.services.DataSetService;
import org.grozeille.bigdata.dataset.services.HiveService;
import org.grozeille.bigdata.dataset.web.dto.CloneCustomFileDataSetRequest;
import org.grozeille.bigdata.dataset.web.dto.CustomFileDataSetRequest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.InputStreamResource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.servlet.support.ServletUriComponentsBuilder;
import springfox.documentation.annotations.ApiIgnore;

import javax.servlet.http.HttpServletResponse;
import java.net.URI;
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

    @RequestMapping(value = "/custom-file/{database}/{table}", method = RequestMethod.PUT)
    public void create(
            @ApiIgnore @ApiParam(hidden = true) Principal principal,
            @PathVariable("database") String database,
            @PathVariable("table") String table,
            @RequestBody CustomFileDataSetRequest request) throws Exception {

        CustomFileDataSetConf customFileDataSetConf = null;

        HiveTable hiveTable = hiveService.findOne(database, table);
        if(hiveTable != null) {
            customFileDataSetConf = this.customFileDataSetService.extractDataSetConf(hiveTable);
        }
        else {
            customFileDataSetConf = new CustomFileDataSetConf();
        }
        customFileDataSetConf.setFileFormat(request.getDataSetConfig().getFileFormat());
        customFileDataSetConf.setFirstLineHeader(request.getDataSetConfig().isFirstLineHeader());
        customFileDataSetConf.setSeparator(request.getDataSetConfig().getSeparator());
        customFileDataSetConf.setTextQualifier(request.getDataSetConfig().getTextQualifier());
        customFileDataSetConf.setSheet(request.getDataSetConfig().getSheet());

        this.customFileDataSetService.createOrUpdateCustomFileTable(
                new DataSetConf(database, table, request.getComment(), request.getTags()),
                principal.getName(),
                request.getTemporary(),
                customFileDataSetConf
        );
    }

    @RequestMapping(value = "/custom-file/{database}/{table}/file", method = RequestMethod.PUT, consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    @Transactional(readOnly = false)
    public void uploadFile(
            @PathVariable("database") String database,
            @PathVariable("table") String table,
            @RequestPart("file") MultipartFile file) throws Exception {

        checkIfExists(database, table);

        customFileDataSetService.uploadFile(
                database,
                table,
                file.getOriginalFilename(),
                file.getContentType(),
                file.getInputStream());
    }

    @RequestMapping(value = "/custom-file/{database}/{table}/update-table-schema", method = RequestMethod.POST)
    @Transactional(readOnly = false)
    public void updateSchema(
            @PathVariable("database") String database,
            @PathVariable("table") String table) throws Exception {

        checkIfExists(database, table);

        customFileDataSetService.updateTableSchema(database, table);
    }

    @RequestMapping(value = "/custom-file/{database}/{table}/file", method = RequestMethod.GET)
    public ResponseEntity<InputStreamResource> downloadFile(
            @PathVariable("database") String database,
            @PathVariable("table") String table,
            HttpServletResponse response) throws Exception {

        checkIfExists(database, table);

        CustomFileDataSetFileStream is = this.customFileDataSetService.downloadFile(database, table);

        String fileName = new Path(is.getFileInfo().getPath()).getName();
        String contentType = is.getFileInfo().getContentType();
        int size = is.getFileInfo().getSize();

        HttpHeaders respHeaders = new HttpHeaders();
        respHeaders.setContentType(MediaType.valueOf(contentType));
        if(size > 0) {
            respHeaders.setContentLength(size);
        }
        respHeaders.setContentDispositionFormData("attachment", fileName);

        // get your file as InputStream
        InputStreamResource isr = new InputStreamResource(is.getInputStream());
        return new ResponseEntity<InputStreamResource>(isr, respHeaders, HttpStatus.OK);
    }

    @RequestMapping(value = "/custom-file/{database}/{table}/file/sheets", method = RequestMethod.GET)
    public String[] excelSheets(@PathVariable("database") String database,
                           @PathVariable("table") String table) throws Exception {
        return this.customFileDataSetService.sheets(database, table);
    }

    @RequestMapping(value = "/custom-file/{database}/{table}/clone", method = RequestMethod.POST)
    public ResponseEntity<?> clone(
            @ApiIgnore @ApiParam(hidden = true) Principal principal,
            @PathVariable("database") String database,
            @PathVariable("table") String table,
            @RequestBody CloneCustomFileDataSetRequest request) throws Exception {

        this.customFileDataSetService.clone(
                database,
                table,
                request.getTargetDatabase(),
                request.getTargetTable(),
                principal.getName(),
                request.getTemporary());

        URI location = ServletUriComponentsBuilder
                .fromCurrentContextPath().path("/api/dataset/custom-file/{database}/{table}")
                .buildAndExpand(request.getTargetDatabase(), request.getTargetTable()).toUri();

        return ResponseEntity.created(location).build();

    }

    private HiveTable checkIfExists(@PathVariable("database") String database, @PathVariable("table") String table) throws TException {
        HiveTable hiveTable = hiveService.findOne(database, table);
        if(hiveTable == null){
            throw new HttpClientErrorException(HttpStatus.BAD_REQUEST, "Table '"+database+"."+table+"' does not exist");
        }
        else if(!DataSetType.CustomFileDataSet.name().equals(hiveTable.getDataSetType())) {
            // test if the table is a customFileDataSet
            throw new HttpClientErrorException(HttpStatus.BAD_REQUEST, "Table '"+database+"."+table+"' is not a custom file dataset");
        }

        return hiveTable;
    }

}
