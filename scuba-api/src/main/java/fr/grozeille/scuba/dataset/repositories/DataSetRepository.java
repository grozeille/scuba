package fr.grozeille.scuba.dataset.repositories;


import fr.grozeille.scuba.configurations.stereotype.SolrRepository;
import fr.grozeille.scuba.dataset.model.DataSetSearchItem;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.solr.repository.Query;
import org.springframework.data.solr.repository.SolrCrudRepository;

import java.util.Collection;

@SolrRepository
public interface DataSetRepository extends SolrCrudRepository<DataSetSearchItem, String> {

    @Query(value = "temporary:false")
    Page<DataSetSearchItem> findAllNotTemporary(Pageable pageable);

    @Query(value = "text:?0 AND temporary:false")
    Page<DataSetSearchItem> findByAll(Pageable pageable, String text);

    DataSetSearchItem findByDatabaseAndTable(String database, String table);

    @Query(value = "dataSetType:(?0) AND temporary:false")
    Page<DataSetSearchItem> findByDatalakeItemTypeIn(Pageable pageable, Collection<String> datalakeItemType);

    @Query(value = "dataSetType:(?0) AND text:?1 AND temporary:false")
    Page<DataSetSearchItem> findByDatalakeItemTypeInAndAll(Pageable pageable, Collection<String> datalakeItemType, String text);
}
