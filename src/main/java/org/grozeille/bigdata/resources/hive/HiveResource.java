package org.grozeille.bigdata.resources.hive;

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
        hiveTable.setCreator(creationRequest.getDataDomainOwner());
        hiveTable.setComment(creationRequest.getComment());
        hiveTable.setTags(creationRequest.getTags());
        hiveTable.setColumns(new HiveColumn[]{ hiveDummyColumn });

        this.hiveService.createOrcTable(hiveTable);
    }


}
