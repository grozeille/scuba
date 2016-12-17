package org.grozeille.bigdata.repositories.jpa;

import org.grozeille.bigdata.resources.userdataset.model.UserDataSet;
import org.springframework.data.repository.CrudRepository;

public interface UserDataSetRepository extends CrudRepository<UserDataSet, String> {
}
