package org.grozeille.bigdata.resources.hive;

import org.grozeille.bigdata.resources.dataset.model.DataSetConf;
import org.grozeille.bigdata.resources.hive.model.HiveData;
import org.grozeille.bigdata.resources.hive.model.HiveTable;
import org.grozeille.bigdata.services.HiveService;
import org.grozeille.bigdata.services.HiveInvalidDataSetException;
import lombok.extern.slf4j.Slf4j;
import org.apache.thrift.TException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;


@RestController
@Slf4j
@RequestMapping("/api/hive")
public class HiveResource {

    @Autowired
    private HiveService hiveService;

    @RequestMapping(value = "/", method = RequestMethod.GET)
    public HiveTable[] tables() throws TException {

        return hiveService.findAllPublicTables();

    }

    @RequestMapping(value = "/{database}/{table}", method = RequestMethod.GET)
    public HiveTable table(@PathVariable("database") String database, @PathVariable("table") String table) throws TException {

        return hiveService.findOne(database, table);
    }

    @RequestMapping(value = "/data", method = RequestMethod.POST)
    public HiveData data(
            @RequestBody DataSetConf dataSetConf,
            @RequestParam(value = "max", required = false, defaultValue = "10000") long max) throws Exception {

        HiveData hiveData = new HiveData();

        try {
            hiveData.setData(hiveService.getData(dataSetConf, max));
        } catch (HiveInvalidDataSetException e) {
            throw e;
        } catch (Exception e) {
            log.error("Unexpected error", e);
            throw new Exception("Unexpected error");
        }

        return hiveData;
    }
}
