package org.grozeille.bigdata.repositories.jpa;

import org.grozeille.bigdata.resources.wranglingdataset.model.WranglingDataSet;
import org.springframework.data.repository.CrudRepository;

public interface UserDataSetRepository extends CrudRepository<WranglingDataSet, String> {
}
