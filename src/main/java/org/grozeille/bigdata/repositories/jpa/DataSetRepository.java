package org.grozeille.bigdata.repositories.jpa;

import org.grozeille.bigdata.resources.dataset.model.DataSet;
import org.springframework.data.repository.CrudRepository;

public interface DataSetRepository extends CrudRepository<DataSet, String> {
}
