package org.grozeille.bigdata.resources.wranglingdataset;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import org.grozeille.bigdata.repositories.jpa.UserDataSetRepository;
import org.grozeille.bigdata.resources.wranglingdataset.model.WranglingDataSet;
import org.grozeille.bigdata.resources.wranglingdataset.model.WranglingDataSetConf;
import org.grozeille.bigdata.resources.wranglingdataset.model.WranglingDataSetCreationResponse;
import org.grozeille.bigdata.services.HiveQueryException;
import org.grozeille.bigdata.services.HiveService;
import io.swagger.annotations.ApiOperation;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.client.HttpClientErrorException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;


@RestController
@Slf4j
@RequestMapping("/api/dataset-wrangling")
public class WranglingDataSetResource {

    @Autowired
    private UserDataSetRepository userDataSetRepository;

    @Autowired
    private HiveService hiveService;

    private final ObjectMapper mapper = new ObjectMapper();

    @RequestMapping(value = "/{database}/{table}", method = RequestMethod.PUT)
    public void createWranglingDataSet(
            @PathVariable("database") String database,
            @PathVariable("table") String table) {

    }

    @RequestMapping(value = "/{database}/{table}/preview", method = RequestMethod.POST)
    public void preview(
            @PathVariable("database") String database,
            @PathVariable("table") String table) {

    }

    @RequestMapping(value = "/{id}", method = RequestMethod.GET)
    public WranglingDataSetConf dataset(@PathVariable("id") String id){

        WranglingDataSet wranglingDataSet = userDataSetRepository.findOne(id);

        if(wranglingDataSet == null) {
            throw new HttpClientErrorException(HttpStatus.NOT_FOUND);
        }

        return readDataSet(wranglingDataSet);
    }

    @RequestMapping(value = "/{id}", method = RequestMethod.PUT)
    public WranglingDataSetCreationResponse dataset(@PathVariable("id") String id, @RequestBody WranglingDataSetConf wranglingDataSetConf) throws HiveQueryException {

        if(!id.equalsIgnoreCase(wranglingDataSetConf.getId())){
            throw new HttpClientErrorException(HttpStatus.BAD_REQUEST, "The ID of DataSet is not the same as in URL");
        }

        // TODO: verify that the name doesn't already exist

        WranglingDataSet wranglingDataSet = userDataSetRepository.findOne(id);

        if(wranglingDataSet == null) {
            throw new HttpClientErrorException(HttpStatus.NOT_FOUND);
        }

        try {
            wranglingDataSet.setJsonConf(mapper.writeValueAsString(wranglingDataSetConf));
        } catch (JsonProcessingException e) {
            log.error("Unable to parse JSON");
            throw new HttpClientErrorException(HttpStatus.BAD_REQUEST, "Unable to parse JSON");
        }

        userDataSetRepository.save(wranglingDataSet);

        hiveService.createDataSet(wranglingDataSetConf);

        return new WranglingDataSetCreationResponse(id);
    }

    @RequestMapping(value = "", method = RequestMethod.PUT)
    @Transactional(readOnly = false)
    public WranglingDataSetCreationResponse dataset(@RequestBody WranglingDataSetConf wranglingDataSetConf) throws HiveQueryException {

        if(!Strings.isNullOrEmpty(wranglingDataSetConf.getId())){
            throw new HttpClientErrorException(HttpStatus.BAD_REQUEST, "The ID should be empty for creation");
        }

        // TODO: verify that the name doesn't already exist

        String id = UUID.randomUUID().toString();
        WranglingDataSet wranglingDataSet = new WranglingDataSet();
        wranglingDataSet.setId(id);
        wranglingDataSetConf.setId(id);

        try {
            wranglingDataSet.setJsonConf(mapper.writeValueAsString(wranglingDataSetConf));
        } catch (JsonProcessingException e) {
            log.error("Unable to parse JSON");
            throw new HttpClientErrorException(HttpStatus.BAD_REQUEST, "Unable to parse JSON");
        }

        userDataSetRepository.save(wranglingDataSet);

        hiveService.createDataSet(wranglingDataSetConf);

        return new WranglingDataSetCreationResponse(id);
    }

    @RequestMapping(value = "/{id}", method = RequestMethod.DELETE)
    public void deleteDataSet(@PathVariable("id") String id) {

        WranglingDataSet wranglingDataSet = userDataSetRepository.findOne(id);

        if(wranglingDataSet == null) {
            throw new HttpClientErrorException(HttpStatus.NOT_FOUND);
        }

        userDataSetRepository.delete(id);
    }

    private WranglingDataSetConf readDataSet(WranglingDataSet wranglingDataSet){
        try {
            return mapper.readValue(wranglingDataSet.getJsonConf(), WranglingDataSetConf.class);
        } catch (IOException e) {
            log.error("Unable to read JSON conf from ID "+ wranglingDataSet.getId(), e);
        }

        return null;
    }
}
