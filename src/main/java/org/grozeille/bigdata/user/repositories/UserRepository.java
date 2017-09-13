package org.grozeille.bigdata.user.repositories;

import org.grozeille.bigdata.configurations.stereotype.JpaRepository;
import org.grozeille.bigdata.user.model.User;
import org.springframework.data.repository.CrudRepository;

@JpaRepository
public interface UserRepository extends CrudRepository<User, String> {
}
