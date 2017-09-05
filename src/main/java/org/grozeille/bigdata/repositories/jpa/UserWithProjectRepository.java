package org.grozeille.bigdata.repositories.jpa;

import org.grozeille.bigdata.resources.user.model.User;
import org.grozeille.bigdata.resources.user.model.UserWithProject;
import org.springframework.data.repository.CrudRepository;

public interface UserWithProjectRepository extends CrudRepository<UserWithProject, String> {
}
