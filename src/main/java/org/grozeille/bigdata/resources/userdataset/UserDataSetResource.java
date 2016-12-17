package org.grozeille.bigdata.resources.userdataset;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import org.grozeille.bigdata.repositories.jpa.UserDataSetRepository;
import org.grozeille.bigdata.resources.userdataset.model.UserDataSet;
import org.grozeille.bigdata.resources.userdataset.model.UserDataSetConf;
import org.grozeille.bigdata.resources.userdataset.model.UserDataSetCreationResponse;
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
@RequestMapping("/api/userdataset")
public class UserDataSetResource {

    @Autowired
    private UserDataSetRepository userDataSetRepository;

    @Autowired
    private HiveService hiveService;

    private final ObjectMapper mapper = new ObjectMapper();

    @ApiOperation(value = "", notes = "retrieve all datasets")
    @RequestMapping(value = "", method = RequestMethod.GET)
    public UserDataSetConf[] dataset(){

        List<UserDataSetConf> result = new ArrayList<>();
        for(UserDataSet userDataSet : userDataSetRepository.findAll()){

            UserDataSetConf userDataSetConf = readDataSet(userDataSet);
            if(userDataSetConf != null) {
                result.add(userDataSetConf);
            }
        }

        return result.toArray(new UserDataSetConf[0]);
    }

    @RequestMapping(value = "/{id}", method = RequestMethod.GET)
    public UserDataSetConf dataset(@PathVariable("id") String id){

        UserDataSet userDataSet = userDataSetRepository.findOne(id);

        if(userDataSet == null) {
            throw new HttpClientErrorException(HttpStatus.NOT_FOUND);
        }

        return readDataSet(userDataSet);
    }

    @RequestMapping(value = "/{id}", method = RequestMethod.PUT)
    public UserDataSetCreationResponse dataset(@PathVariable("id") String id, @RequestBody UserDataSetConf userDataSetConf) throws HiveQueryException {

        if(!id.equalsIgnoreCase(userDataSetConf.getId())){
            throw new HttpClientErrorException(HttpStatus.BAD_REQUEST, "The ID of DataSet is not the same as in URL");
        }

        // TODO: verify that the name doesn't already exist

        UserDataSet userDataSet = userDataSetRepository.findOne(id);

        if(userDataSet == null) {
            throw new HttpClientErrorException(HttpStatus.NOT_FOUND);
        }

        try {
            userDataSet.setJsonConf(mapper.writeValueAsString(userDataSetConf));
        } catch (JsonProcessingException e) {
            log.error("Unable to parse JSON");
            throw new HttpClientErrorException(HttpStatus.BAD_REQUEST, "Unable to parse JSON");
        }

        userDataSetRepository.save(userDataSet);

        hiveService.createDataSet(userDataSetConf);

        return new UserDataSetCreationResponse(id);
    }

    @RequestMapping(value = "", method = RequestMethod.PUT)
    @Transactional(readOnly = false)
    public UserDataSetCreationResponse dataset(@RequestBody UserDataSetConf userDataSetConf) throws HiveQueryException {

        if(!Strings.isNullOrEmpty(userDataSetConf.getId())){
            throw new HttpClientErrorException(HttpStatus.BAD_REQUEST, "The ID should be empty for creation");
        }

        // TODO: verify that the name doesn't already exist

        String id = UUID.randomUUID().toString();
        UserDataSet userDataSet = new UserDataSet();
        userDataSet.setId(id);
        userDataSetConf.setId(id);

        try {
            userDataSet.setJsonConf(mapper.writeValueAsString(userDataSetConf));
        } catch (JsonProcessingException e) {
            log.error("Unable to parse JSON");
            throw new HttpClientErrorException(HttpStatus.BAD_REQUEST, "Unable to parse JSON");
        }

        userDataSetRepository.save(userDataSet);

        hiveService.createDataSet(userDataSetConf);

        return new UserDataSetCreationResponse(id);
    }

    @RequestMapping(value = "/{id}", method = RequestMethod.DELETE)
    public void deleteDataSet(@PathVariable("id") String id) {

        UserDataSet userDataSet = userDataSetRepository.findOne(id);

        if(userDataSet == null) {
            throw new HttpClientErrorException(HttpStatus.NOT_FOUND);
        }

        userDataSetRepository.delete(id);
    }

    private UserDataSetConf readDataSet(UserDataSet userDataSet){
        try {
            return mapper.readValue(userDataSet.getJsonConf(), UserDataSetConf.class);
        } catch (IOException e) {
            log.error("Unable to read JSON conf from ID "+ userDataSet.getId(), e);
        }

        return null;
    }
}
