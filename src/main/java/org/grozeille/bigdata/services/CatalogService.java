package org.grozeille.bigdata.services;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.thrift.TException;
import org.grozeille.bigdata.resources.dataset.model.DataSet;
import org.grozeille.bigdata.repositories.jpa.UserDataSetRepository;
import org.grozeille.bigdata.repositories.solr.DataSetRepository;
import org.grozeille.bigdata.resources.hive.model.HiveColumn;
import org.grozeille.bigdata.resources.hive.model.HiveTable;
import org.grozeille.bigdata.resources.userdataset.model.UserDataSet;
import org.grozeille.bigdata.resources.userdataset.model.UserDataSetConf;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

@Service
@Slf4j
public class CatalogService {

    @Autowired
    private HiveService hiveService;

    @Autowired
    private UserDataSetRepository userDataSetRepository;

    @Autowired
    private DataSetRepository dataSetRepository;

    private final ObjectMapper objectMapper = new ObjectMapper();

    public void refreshCatalog() throws TException {

        // get all indexed
        // TODO: find a way to not load all data in memory
        Iterable<DataSet> datasetItems = dataSetRepository.findAll();
        Set<String> tableKeys = StreamSupport
                .stream(datasetItems.spliterator(), false)
                .map(h -> h.getId())
                .collect(Collectors.toSet());

        // load all hive tables
        Stream<HiveTable> allPublicTables = hiveService.findAllPublicTables();

        // refresh index from tables
        allPublicTables.forEach(hiveTable -> {

            DataSet dataSet = convert(hiveTable);

            // index the table
            dataSetRepository.save(dataSet);

            // remove it from the list of existing tables
            tableKeys.remove(dataSet.getId());
        });

        // load datasets
        Iterable<UserDataSet> userDataSets = userDataSetRepository.findAll();
        for (UserDataSet uds : userDataSets) {

            try {
                UserDataSetConf userDataSetConf = objectMapper.readValue(uds.getJsonConf(), UserDataSetConf.class);

                DataSet dataSet = convert(userDataSetConf);

                // index the table
                dataSetRepository.save(dataSet);

                // remove it from the list of existing tables
                tableKeys.remove(dataSet.getId());

            } catch (IOException e) {
                log.error("Unable to read user DataSet: " + uds.getId(), e);
            }
        }

        // delete missing tables
        for (String key : tableKeys) {
            dataSetRepository.delete(key);
        }
    }

    public void refreshTable(String database, String table){
        try {
            HiveTable hiveTable = hiveService.findOne(database, table);

            DataSet dataSet = convert(hiveTable);

            // index the table
            dataSetRepository.save(dataSet);
        } catch (TException e) {
            log.error("Unable to refresh table "+database+"."+table, e);
        }
    }


    public void refreshUserDataSet(UserDataSetConf userDataSetConf){
        DataSet dataSet = convert(userDataSetConf);

        // index the table
        dataSetRepository.save(dataSet);
    }

    private DataSet convert(HiveTable hiveTable) {
        DataSet dataSet = new DataSet();
        dataSet.setId("`" + hiveTable.getDatabase() + "`.`" + hiveTable.getTable() + "`");
        dataSet.setComment(hiveTable.getComment());
        dataSet.setDatabase(hiveTable.getDatabase());
        dataSet.setTable(hiveTable.getTable());
        dataSet.setDataDomainOwner(hiveTable.getDataDomainOwner());
        dataSet.setFormat(hiveTable.getFormat());
        dataSet.setPath(hiveTable.getPath());
        dataSet.setTags(hiveTable.getTags());
        dataSet.setDatalakeItemType(hiveTable.getDatalakeItemType());

        dataSet.setColumns(Arrays.stream(hiveTable.getColumns()).map(c -> c.getName()).toArray(String[]::new));
        dataSet.setColumnsComment(Arrays.stream(hiveTable.getColumns()).map(c -> c.getDescription()).toArray(String[]::new));

        try {
            dataSet.setJsonData(objectMapper.writeValueAsString(hiveTable));
        } catch (JsonProcessingException e) {
            log.error("Unable to read JSON from solr for table: " + dataSet.getId(), e);
        }

        return dataSet;
    }

    private DataSet convert(UserDataSetConf userDataSetConf){

        HiveTable hiveTable = new HiveTable();
        hiveTable.setDatabase(userDataSetConf.getDatabase());
        hiveTable.setTable(userDataSetConf.getTable());
        hiveTable.setComment(userDataSetConf.getComment());
        hiveTable.setDataDomainOwner(userDataSetConf.getDataDomainOwner());
        hiveTable.setFormat(userDataSetConf.getFormat());
        hiveTable.setPath(userDataSetConf.getPath());
        hiveTable.setTags(userDataSetConf.getTags());
        hiveTable.setDatalakeItemType(HiveService.DATALAKE_ITEM_TYPE_USER_DATASET);

        Stream<HiveColumn> calculatedColumns = Arrays.stream(userDataSetConf.getCalculatedColumns()).map(c -> {
            HiveColumn column = new HiveColumn();
            column.setName(c.getNewName());
            column.setDescription(c.getNewDescription());
            column.setType(c.getNewType());
            return column;
        });
        Stream<HiveColumn> selectedColumns = Arrays.stream(userDataSetConf.getTables()).flatMap(t -> {
            return Arrays.stream(t.getColumns())
                    .map(c -> {
                        if(!c.getSelected()){
                            return null;
                        }

                        HiveColumn column = new HiveColumn();
                        column.setName(c.getNewName());
                        column.setDescription(c.getNewDescription());
                        column.setType(c.getNewType());
                        return column;
                    })
                    .filter(Objects::nonNull);
        });

        HiveColumn[] allColumns = Stream.concat(calculatedColumns, selectedColumns).toArray(HiveColumn[]::new);
        hiveTable.setColumns(allColumns);

        return convert(hiveTable);
    }
}