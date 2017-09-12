package org.grozeille.bigdata.resources.dataset;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import lombok.extern.slf4j.Slf4j;
import org.grozeille.bigdata.resources.dataset.model.DataSet;
import org.grozeille.bigdata.repositories.solr.DataSetRepository;
import org.grozeille.bigdata.resources.hive.model.HiveTable;
import org.grozeille.bigdata.services.DataSetService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

@RestController
@Slf4j
@RequestMapping("/api/dataset")
public class DataSetResource {

    @Autowired
    private DataSetRepository dataSetRepository;

    @Autowired
    private DataSetService dataSetService;

    @RequestMapping(value = "", method = RequestMethod.GET)
    public Iterable<HiveTable> filter(
            Pageable pageable,
            @RequestParam(value = "filter", required = false, defaultValue = "") String filter,
            @RequestParam(value = "datalakeItemType", required = false) Collection<String> datalakeItemType) {

        final ObjectMapper objectMapper = new ObjectMapper();
        Page<DataSet> result;
        if(Strings.isNullOrEmpty(filter)) {
            if(datalakeItemType == null || datalakeItemType.isEmpty()) {
                result = dataSetRepository.findAll(pageable);
            }
            else {
                result = dataSetRepository.findByDatalakeItemTypeIn(pageable, datalakeItemType);
            }
        }
        else {
            if(datalakeItemType == null || datalakeItemType.isEmpty()) {
                result = dataSetRepository.findByAll(pageable, filter);
            }
            else {
                result = dataSetRepository.findByDatalakeItemTypeInAndAll(pageable, datalakeItemType, filter);
            }
        }

        return result.map(dataSet -> {
            try {
                return objectMapper.readValue(dataSet.getJsonData(), HiveTable.class);
            } catch (IOException e) {
                log.error("Unable to read json data: "+dataSet.getJsonData());
                return null;
            }
        });
    }

    @RequestMapping(value = "/{database}/{table}", method = RequestMethod.GET)
    public HiveTable get(@PathVariable("database") String database,
                         @PathVariable("table") String table) {
        final ObjectMapper objectMapper = new ObjectMapper();

        DataSet result = dataSetRepository.findByDatabaseAndTable(database, table);
        try {
            return objectMapper.readValue(result.getJsonData(), HiveTable.class);
        } catch (IOException e) {
            log.error("Unable to read json data: "+result.getJsonData());
            return null;
        }
    }

    @RequestMapping(value = "/{database}/{table}", method = RequestMethod.DELETE)
    public void delete(
            @PathVariable("database") String database,
            @PathVariable("table") String table) throws Exception {

        this.dataSetService.delete(database, table);
    }

    @RequestMapping(value = "/refresh", method = RequestMethod.POST)
    public void refresh() throws Exception {
        dataSetService.refreshAll();
    }
}
