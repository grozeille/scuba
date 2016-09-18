package org.grozeille.bigdata.resources.dataset;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import org.grozeille.bigdata.repositories.DataSetRepository;
import org.grozeille.bigdata.resources.dataset.model.DataSet;
import org.grozeille.bigdata.resources.dataset.model.DataSetConf;
import org.grozeille.bigdata.resources.dataset.model.DataSetCreationResponse;
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
@RequestMapping("/api/dataset")
public class DataSetResource {

    @Autowired
    private DataSetRepository dataSetRepository;

    @Autowired
    private HiveService hiveService;

    private final ObjectMapper mapper = new ObjectMapper();

    @ApiOperation(value = "", notes = "retrieve all datasets")
    @RequestMapping(value = "", method = RequestMethod.GET)
    public DataSetConf[] dataset(){

        List<DataSetConf> result = new ArrayList<>();
        for(DataSet dataSet : dataSetRepository.findAll()){

            DataSetConf dataSetConf = readDataSet(dataSet);
            if(dataSetConf != null) {
                result.add(dataSetConf);
            }
        }

        return result.toArray(new DataSetConf[0]);
    }

    @RequestMapping(value = "/{id}", method = RequestMethod.GET)
    public DataSetConf dataset(@PathVariable("id") String id){

        DataSet dataSet = dataSetRepository.findOne(id);

        if(dataSet == null) {
            throw new HttpClientErrorException(HttpStatus.NOT_FOUND);
        }

        return readDataSet(dataSet);
    }

    @RequestMapping(value = "/{id}", method = RequestMethod.PUT)
    public DataSetCreationResponse dataset(@PathVariable("id") String id, @RequestBody DataSetConf dataSetConf) throws HiveQueryException {

        if(!id.equalsIgnoreCase(dataSetConf.getId())){
            throw new HttpClientErrorException(HttpStatus.BAD_REQUEST, "The ID of DataSet is not the same as in URL");
        }

        // TODO: verify that the name doesn't already exist

        DataSet dataSet = dataSetRepository.findOne(id);

        if(dataSet == null) {
            throw new HttpClientErrorException(HttpStatus.NOT_FOUND);
        }

        try {
            dataSet.setJsonConf(mapper.writeValueAsString(dataSetConf));
        } catch (JsonProcessingException e) {
            log.error("Unable to parse JSON");
            throw new HttpClientErrorException(HttpStatus.BAD_REQUEST, "Unable to parse JSON");
        }

        dataSetRepository.save(dataSet);

        hiveService.createDataSet(dataSetConf);

        return new DataSetCreationResponse(id);
    }

    @RequestMapping(value = "", method = RequestMethod.PUT)
    @Transactional(readOnly = false)
    public DataSetCreationResponse dataset(@RequestBody DataSetConf dataSetConf) throws HiveQueryException {

        if(!Strings.isNullOrEmpty(dataSetConf.getId())){
            throw new HttpClientErrorException(HttpStatus.BAD_REQUEST, "The ID should be empty for creation");
        }

        // TODO: verify that the name doesn't already exist

        String id = UUID.randomUUID().toString();
        DataSet dataSet = new DataSet();
        dataSet.setId(id);
        dataSetConf.setId(id);

        try {
            dataSet.setJsonConf(mapper.writeValueAsString(dataSetConf));
        } catch (JsonProcessingException e) {
            log.error("Unable to parse JSON");
            throw new HttpClientErrorException(HttpStatus.BAD_REQUEST, "Unable to parse JSON");
        }

        dataSetRepository.save(dataSet);

        hiveService.createDataSet(dataSetConf);

        return new DataSetCreationResponse(id);
    }

    @RequestMapping(value = "/{id}", method = RequestMethod.DELETE)
    public void deleteDataSet(@PathVariable("id") String id) {

        DataSet dataSet = dataSetRepository.findOne(id);

        if(dataSet == null) {
            throw new HttpClientErrorException(HttpStatus.NOT_FOUND);
        }

        dataSetRepository.delete(id);
    }

    private DataSetConf readDataSet(DataSet dataSet){
        try {
            return mapper.readValue(dataSet.getJsonConf(), DataSetConf.class);
        } catch (IOException e) {
            log.error("Unable to read JSON conf from ID "+dataSet.getId(), e);
        }

        return null;
    }
}
