package org.grozeille.bigdata.dataset.repositories;


import org.grozeille.bigdata.configurations.stereotype.SolrRepository;
import org.grozeille.bigdata.dataset.model.DataSetSearchItem;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.solr.repository.Query;
import org.springframework.data.solr.repository.SolrCrudRepository;

import java.util.Collection;

@SolrRepository
public interface DataSetRepository extends SolrCrudRepository<DataSetSearchItem, String> {

    @Query(value = "text:?0")
    Page<DataSetSearchItem> findByAll(Pageable pageable, String text);

    DataSetSearchItem findByDatabaseAndTable(String database, String table);

    @Query(value = "dataSetType:(?0)")
    Page<DataSetSearchItem> findByDatalakeItemTypeIn(Pageable pageable, Collection<String> datalakeItemType);

    @Query(value = "dataSetType:(?0) AND text:?1")
    Page<DataSetSearchItem> findByDatalakeItemTypeInAndAll(Pageable pageable, Collection<String> datalakeItemType, String text);
}
