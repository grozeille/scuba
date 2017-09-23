package org.grozeille.bigdata.dataset.web;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import lombok.extern.slf4j.Slf4j;
import org.apache.thrift.TException;
import org.grozeille.bigdata.dataset.model.DataSetSearchItem;
import org.grozeille.bigdata.dataset.repositories.DataSetRepository;
import org.grozeille.bigdata.dataset.web.dto.DataSetData;
import org.grozeille.bigdata.dataset.model.HiveTable;
import org.grozeille.bigdata.dataset.services.DataSetService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.Collection;

@RestController
@Slf4j
@RequestMapping("/api/dataset")
public class DataSetResource {

    private static final String DEFAULT_MAX = "50000";

    @Autowired
    private DataSetRepository dataSetRepository;

    @Autowired
    private DataSetService dataSetService;

    private final ThreadPoolTaskExecutor refreshThreadPoolTaskExecutor;

    public DataSetResource() {
        refreshThreadPoolTaskExecutor = new ThreadPoolTaskExecutor();
        refreshThreadPoolTaskExecutor.setMaxPoolSize(1);
        refreshThreadPoolTaskExecutor.setQueueCapacity(1);
        refreshThreadPoolTaskExecutor.setCorePoolSize(1);
        refreshThreadPoolTaskExecutor.initialize();
    }

    @ApiImplicitParams({
            @ApiImplicitParam(name = "page", dataType = "integer", paramType = "query",
                    value = "Results page you want to retrieve (0..N)"),
            @ApiImplicitParam(name = "size", dataType = "integer", paramType = "query",
                    value = "Number of records per page."),
            @ApiImplicitParam(name = "sort", allowMultiple = true, dataType = "string", paramType = "query",
                    value = "Sorting criteria in the format: property(,asc|desc). " +
                            "Default sort order is ascending. " +
                            "Multiple sort criteria are supported.")
    })
    @RequestMapping(value = "", method = RequestMethod.GET)
    public Iterable<HiveTable> filter(
            Pageable pageable,
            @RequestParam(value = "filter", required = false, defaultValue = "") String filter,
            @RequestParam(value = "dataSetType", required = false) Collection<String> datalakeItemType) {

        final ObjectMapper objectMapper = new ObjectMapper();
        Page<DataSetSearchItem> result;
        if(Strings.isNullOrEmpty(filter)) {
            if(datalakeItemType == null || datalakeItemType.isEmpty()) {
                result = dataSetRepository.findAllNotTemporary(pageable);
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

        return result.map(dataSetSearchItem -> {
            try {
                return objectMapper.readValue(dataSetSearchItem.getJsonData(), HiveTable.class);
            } catch (IOException e) {
                log.error("Unable to read json data: "+ dataSetSearchItem.getJsonData());
                return null;
            }
        });
    }

    @RequestMapping(value = "/{database}/{table}", method = RequestMethod.GET)
    public ResponseEntity<HiveTable> get(@PathVariable("database") String database,
                                         @PathVariable("table") String table) throws IOException {
        final ObjectMapper objectMapper = new ObjectMapper();

        DataSetSearchItem result = dataSetRepository.findByDatabaseAndTable(database, table);
        if(result == null) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body(null);
        }
        return ResponseEntity.ok(objectMapper.readValue(result.getJsonData(), HiveTable.class));
    }

    @RequestMapping(value = "/{database}/{table}", method = RequestMethod.DELETE)
    public void delete(
            @PathVariable("database") String database,
            @PathVariable("table") String table) throws Exception {

        this.dataSetService.delete(database, table);
    }

    @RequestMapping(value = "/{database}/{table}/data", method = RequestMethod.GET)
    public DataSetData data(
            @PathVariable("database") String database,
            @PathVariable("table") String table,
            @RequestParam(name = "max", defaultValue = DEFAULT_MAX) Integer max,
            @RequestParam(name = "useTablePrefix", defaultValue = "true") Boolean useTablePrefix) throws Exception {

        DataSetData data = new DataSetData();
        data.setData(this.dataSetService.getData(database, table, max, useTablePrefix));
        return data;
    }

    @RequestMapping(value = "/refresh", method = RequestMethod.POST)
    @ResponseStatus(HttpStatus.ACCEPTED)
    public ResponseEntity<Void> refresh() throws Exception {

        refreshThreadPoolTaskExecutor.execute(() -> {
            try {
                dataSetService.refreshAll();
            } catch (TException e) {
                log.error("Error during refresh", e);
            }
        });

        return ResponseEntity.accepted().build();
    }
}
