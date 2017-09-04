package org.grozeille.bigdata.repositories.jpa;

import org.grozeille.bigdata.resources.user.model.User;
import org.springframework.data.repository.CrudRepository;

public interface UserRepository extends CrudRepository<User, String> {
}
