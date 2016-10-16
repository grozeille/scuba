package org.grozeille.bigdata.repositories.solr;


import org.grozeille.bigdata.resources.hive.model.HiveTableSearch;
import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.Repository;

import java.util.List;

public interface HiveTableSearchRepository extends CrudRepository<HiveTableSearch, String> {
    //List<HiveTableSearch> findByAll(String all);
}
