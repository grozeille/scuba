package org.grozeille.bigdata.repositories.solr;


import org.grozeille.bigdata.resources.dataset.model.DataSet;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.solr.repository.Query;
import org.springframework.data.solr.repository.SolrCrudRepository;

import java.util.Collection;

public interface DataSetRepository extends SolrCrudRepository<DataSet, String> {

    @Query(value = "text:?0")
    Page<DataSet> findByAll(Pageable pageable, String text);

    DataSet findByDatabaseAndTable(String database, String table);

    @Query(value = "datalakeItemType:(?0)")
    Page<DataSet> findByDatalakeItemTypeIn(Pageable pageable, Collection<String> datalakeItemType);

    @Query(value = "datalakeItemType:(?0) AND text:?1")
    Page<DataSet> findByDatalakeItemTypeInAndAll(Pageable pageable, Collection<String> datalakeItemType, String text);
}
