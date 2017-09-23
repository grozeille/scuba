package org.grozeille.bigdata.dataset.web;

import io.swagger.annotations.ApiParam;
import org.grozeille.bigdata.dataset.model.DataSetConf;
import org.grozeille.bigdata.dataset.services.WranglingDataSetService;
import lombok.extern.slf4j.Slf4j;
import org.grozeille.bigdata.dataset.web.dto.CloneDataSetRequest;
import org.grozeille.bigdata.dataset.web.dto.DataSetData;
import org.grozeille.bigdata.dataset.web.dto.WranglingDataSetRequest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.support.ServletUriComponentsBuilder;
import springfox.documentation.annotations.ApiIgnore;

import java.net.URI;
import java.security.Principal;


@RestController
@Slf4j
@RequestMapping("/api/dataset/wrangling")
public class WranglingDataSetResource {

    private static final String DEFAULT_MAX = "50000";

    @Autowired
    private WranglingDataSetService wranglingDataSetService;

    @RequestMapping(value = "/{database}/{table}", method = RequestMethod.PUT)
    public void create(
            @ApiIgnore @ApiParam(hidden = true) Principal principal,
            @PathVariable("database") String database,
            @PathVariable("table") String table,
            @RequestBody WranglingDataSetRequest request) throws Exception {

        this.wranglingDataSetService.createOrUpdateWranglingView(
                new DataSetConf(database, table, request.getComment(), request.getTags()),
                principal.getName(),
                request.getTemporary(),
                request.getDataSetConfig()
        );
    }

    @RequestMapping(value = "/{database}/{table}/preview", method = RequestMethod.GET)
    public DataSetData preview(
            @PathVariable("database") String database,
            @PathVariable("table") String table,
            @RequestParam(name = "max", defaultValue = DEFAULT_MAX) Integer max) throws Exception {

        DataSetData data = new DataSetData();
        data.setData(this.wranglingDataSetService.getPreviewData(database, table, max));
        return data;
    }

    @RequestMapping(value = "/{database}/{table}/clone", method = RequestMethod.POST)
    public ResponseEntity<?> clone(
            @ApiIgnore @ApiParam(hidden = true) Principal principal,
            @PathVariable("database") String database,
            @PathVariable("table") String table,
            @RequestBody CloneDataSetRequest request) throws Exception {

        this.wranglingDataSetService.clone(
                database,
                table,
                request.getTargetDatabase(),
                request.getTargetTable(),
                principal.getName(),
                request.getTemporary());

        URI location = ServletUriComponentsBuilder
                .fromCurrentContextPath().path("/api/dataset/wrangling/{database}/{table}")
                .buildAndExpand(request.getTargetDatabase(), request.getTargetTable()).toUri();

        return ResponseEntity.created(location).build();
    }
}
