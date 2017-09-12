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
import org.grozeille.bigdata.resources.wranglingdataset.model.WranglingDataSet;
import org.grozeille.bigdata.resources.wranglingdataset.model.WranglingDataSetConf;
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
public class DataSetService {

    @Autowired
    private HiveService hiveService;

    @Autowired
    private UserDataSetRepository userDataSetRepository;

    @Autowired
    private HdfsService hdfsService;

    @Autowired
    private DataSetRepository dataSetRepository;

    private final ObjectMapper objectMapper = new ObjectMapper();

    public void refreshAll() throws TException {

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

            DataSet dataSet = refreshTable(hiveTable.getDatabase(), hiveTable.getTable());

            if(dataSet != null) {
                // remove it from the list of existing tables
                tableKeys.remove(dataSet.getId());
            }
        });

        // delete missing tables
        for (String key : tableKeys) {
            dataSetRepository.delete(key);
        }
    }

    public DataSet refreshTable(String database, String table){
        try {
            HiveTable hiveTable = hiveService.findOne(database, table);

            if(!hiveTable.getTemporary()) {
                DataSet dataSet = convert(hiveTable);

                // index the table
                dataSetRepository.save(dataSet);

                return dataSet;
            }

            return null;
        } catch (TException e) {
            log.error("Unable to refresh table "+database+"."+table, e);
            return null;
        }
    }

    public void refreshUserDataSet(WranglingDataSetConf wranglingDataSetConf){
        DataSet dataSet = convert(wranglingDataSetConf);

        // index the table
        dataSetRepository.save(dataSet);
    }

    public void delete(String database, String table) throws Exception {
        HiveTable hiveTable = this.hiveService.findOne(database, table);
        this.hiveService.delete(hiveTable);
        this.hdfsService.delete(hiveTable.getOriginalFile());
        DataSet dataSet = this.dataSetRepository.findByDatabaseAndTable(database, table);
        this.dataSetRepository.delete(dataSet);
    }

    private DataSet convert(HiveTable hiveTable) {
        DataSet dataSet = new DataSet();
        dataSet.setId("`" + hiveTable.getDatabase() + "`.`" + hiveTable.getTable() + "`");
        dataSet.setComment(hiveTable.getComment());
        dataSet.setDatabase(hiveTable.getDatabase());
        dataSet.setTable(hiveTable.getTable());
        dataSet.setCreator(hiveTable.getCreator());
        dataSet.setFormat(hiveTable.getFormat());
        dataSet.setPath(hiveTable.getPath());
        dataSet.setTags(hiveTable.getTags());
        dataSet.setDatalakeItemType(hiveTable.getDatalakeItemType());

        if(hiveTable.getColumns() != null) {
            dataSet.setColumns(Arrays.stream(hiveTable.getColumns()).map(c -> c.getName()).toArray(String[]::new));
            dataSet.setColumnsComment(Arrays.stream(hiveTable.getColumns()).map(c -> c.getDescription()).toArray(String[]::new));
        }

        try {
            dataSet.setJsonData(objectMapper.writeValueAsString(hiveTable));
        } catch (JsonProcessingException e) {
            log.error("Unable to read JSON from solr for table: " + dataSet.getId(), e);
        }

        return dataSet;
    }

    private DataSet convert(WranglingDataSetConf wranglingDataSetConf){

        HiveTable hiveTable = new HiveTable();
        hiveTable.setDatabase(wranglingDataSetConf.getDatabase());
        hiveTable.setTable(wranglingDataSetConf.getTable());
        hiveTable.setComment(wranglingDataSetConf.getComment());
        hiveTable.setCreator(wranglingDataSetConf.getDataDomainOwner());
        hiveTable.setFormat(wranglingDataSetConf.getFormat());
        hiveTable.setPath(wranglingDataSetConf.getPath());
        hiveTable.setTags(wranglingDataSetConf.getTags());
        hiveTable.setDatalakeItemType(HiveService.DATALAKE_ITEM_TYPE_WRANGLING_DATA_SET);

        Stream<HiveColumn> calculatedColumns = Arrays.stream(wranglingDataSetConf.getCalculatedColumns()).map(c -> {
            HiveColumn column = new HiveColumn();
            column.setName(c.getNewName());
            column.setDescription(c.getNewDescription());
            column.setType(c.getNewType());
            return column;
        });
        Stream<HiveColumn> selectedColumns = Arrays.stream(wranglingDataSetConf.getTables()).flatMap(t -> {
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