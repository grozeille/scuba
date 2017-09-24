package fr.grozeille.scuba.dataset.services;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import fr.grozeille.scuba.dataset.model.CustomFileDataSetConf;
import fr.grozeille.scuba.dataset.model.DataSetSearchItem;
import fr.grozeille.scuba.dataset.model.DataSetType;
import fr.grozeille.scuba.dataset.model.HiveTable;
import fr.grozeille.scuba.dataset.repositories.DataSetRepository;
import lombok.extern.slf4j.Slf4j;
import org.apache.thrift.TException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
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
    private HdfsService hdfsService;

    @Autowired
    private DataSetRepository dataSetRepository;

    private final ObjectMapper objectMapper = new ObjectMapper();

    public void refreshAll() throws TException {

        // get all indexed
        // TODO: find a way to not load all data in memory
        Iterable<DataSetSearchItem> datasetItems = dataSetRepository.findAll();
        Set<String> tableKeys = StreamSupport
                .stream(datasetItems.spliterator(), false)
                .map(h -> h.getId())
                .collect(Collectors.toSet());

        // load all hive tables
        Stream<HiveTable> allPublicTables = hiveService.findAllPublicTables();

        // refresh index from tables
        allPublicTables.forEach(hiveTable -> {

            DataSetSearchItem dataSetSearchItem = refreshTable(hiveTable.getDatabase(), hiveTable.getTable());

            if(dataSetSearchItem != null) {
                // remove it from the list of existing tables
                tableKeys.remove(dataSetSearchItem.getId());
            }
        });

        // deleteTable missing tables
        for (String key : tableKeys) {
            dataSetRepository.delete(key);
        }
    }

    public DataSetSearchItem refreshTable(String database, String table){
        try {
            HiveTable hiveTable = hiveService.findOne(database, table);

            DataSetSearchItem dataSetSearchItem = convert(hiveTable);

            // index the table
            dataSetRepository.save(dataSetSearchItem);

            return dataSetSearchItem;
        } catch (TException e) {
            log.error("Unable to refresh table "+database+"."+table, e);
            return null;
        }
    }

    public void delete(String database, String table) throws Exception {
        HiveTable hiveTable = this.hiveService.findOne(database, table);
        this.hiveService.deleteTable(hiveTable);
        if(hiveTable.getDataSetType().equalsIgnoreCase(DataSetType.CustomFileDataSet.name())) {
            CustomFileDataSetConf config = objectMapper.readValue(hiveTable.getDataSetConfiguration(), CustomFileDataSetConf.class);
            this.hdfsService.delete(config.getOriginalFile().getPath());
        }
        DataSetSearchItem dataSetSearchItem = this.dataSetRepository.findByDatabaseAndTable(database, table);
        this.dataSetRepository.delete(dataSetSearchItem);
    }

    public List<Map<String, Object>> getData(String database, String table, Integer max, Boolean useTablePrefix) {
        return this.hiveService.getData(database, table, max, useTablePrefix);
    }

    private DataSetSearchItem convert(HiveTable hiveTable) {
        DataSetSearchItem dataSetSearchItem = new DataSetSearchItem();
        dataSetSearchItem.setId("`" + hiveTable.getDatabase() + "`.`" + hiveTable.getTable() + "`");
        dataSetSearchItem.setComment(hiveTable.getComment());
        dataSetSearchItem.setDatabase(hiveTable.getDatabase());
        dataSetSearchItem.setTable(hiveTable.getTable());
        dataSetSearchItem.setCreator(hiveTable.getCreator());
        dataSetSearchItem.setFormat(hiveTable.getFormat());
        dataSetSearchItem.setPath(hiveTable.getPath());
        dataSetSearchItem.setTags(hiveTable.getTags());
        dataSetSearchItem.setDataSetType(hiveTable.getDataSetType());
        dataSetSearchItem.setTemporary(hiveTable.getTemporary());

        if(hiveTable.getColumns() != null) {
            dataSetSearchItem.setColumns(Arrays.stream(hiveTable.getColumns()).map(c -> c.getName()).toArray(String[]::new));
            dataSetSearchItem.setColumnsComment(Arrays.stream(hiveTable.getColumns()).map(c -> c.getDescription()).toArray(String[]::new));
        }

        try {
            dataSetSearchItem.setJsonData(objectMapper.writeValueAsString(hiveTable));
        } catch (JsonProcessingException e) {
            log.error("Unable to read JSON from solr for table: " + dataSetSearchItem.getId(), e);
        }

        return dataSetSearchItem;
    }
}