package org.grozeille.bigdata.resources.hive;

import org.grozeille.bigdata.resources.dataset.model.DataSetConf;
import org.grozeille.bigdata.resources.hive.model.HiveData;
import org.grozeille.bigdata.resources.hive.model.HiveTable;
import org.grozeille.bigdata.services.CsvParserService;
import org.grozeille.bigdata.services.ExcelParserService;
import org.grozeille.bigdata.services.HiveService;
import org.grozeille.bigdata.services.HiveInvalidDataSetException;
import lombok.extern.slf4j.Slf4j;
import org.apache.thrift.TException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;


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

    @RequestMapping(value = "/excel/sheets", method = RequestMethod.POST)
    public String[] sheets(@RequestParam("file") MultipartFile file) throws Exception {
        return this.excelParserService.sheets(file);
    }

    @RequestMapping(value = "/excel/data", method = RequestMethod.POST)
    public HiveData data(@RequestParam("file") MultipartFile file,
                         @RequestParam(value = "sheet", required = true) String sheet,
                         @RequestParam(value = "firstLineHeader", required = true, defaultValue = "false") boolean firstLineHeader) throws Exception {

        return this.excelParserService.data(file, sheet, firstLineHeader, 5000l);
    }

    @RequestMapping(value = "/csv/data", method = RequestMethod.POST)
    public HiveData data(@RequestParam("file") MultipartFile file,
                         @RequestParam(value = "separator", required = true) Character separator,
                         @RequestParam(value = "textQualifier", required = true, defaultValue = "") Character textQualifier,
                         @RequestParam(value = "firstLineHeader", required = true, defaultValue = "false") boolean firstLineHeader) throws Exception {

        return this.csvParserService.data(file, separator, textQualifier, firstLineHeader, 5000l);
    }

    @RequestMapping(value = "/raw/data", method = RequestMethod.POST)
    public HiveData data(@RequestParam("file") MultipartFile file) throws Exception {

        HiveData result = new HiveData();
        result.setData(new ArrayList<>());
        final String column = "raw";

        try (BufferedReader br = new BufferedReader(new InputStreamReader(file.getInputStream()))) {
            String line;
            while ((line = br.readLine()) != null) {
                Map<String, Object> row = new HashMap<>(1);
                row.put(column, line);
                result.getData().add(row);
            }
        }

        return result;
    }

}
