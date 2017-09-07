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
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;

@RestController
@Slf4j
@RequestMapping("/api/dataset")
public class DataSetResource {

    @Autowired
    private DataSetRepository dataSetRepository;

    @Autowired
    private DataSetService dataSetService;

    @RequestMapping(value = "", method = RequestMethod.GET)
    public Iterable<HiveTable> dataset(Pageable pageable, @RequestParam(value = "filter", required = false, defaultValue = "") String filter) {
        final ObjectMapper objectMapper = new ObjectMapper();
        Page<DataSet> result;
        if(Strings.isNullOrEmpty(filter)) {
            result = dataSetRepository.findAll(pageable);
            return result.map(dataSet -> {
                try {
                    return objectMapper.readValue(dataSet.getJsonData(), HiveTable.class);
                } catch (IOException e) {
                    log.error("Unable to read json data: "+dataSet.getJsonData());
                    return null;
                }
            });
        }
        else {
            result = dataSetRepository.findByAll(pageable, filter);
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

    @RequestMapping(value = "/refresh", method = RequestMethod.POST)
    public void refresh() throws Exception {
        dataSetService.refreshAll();
    }
}
