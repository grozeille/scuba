package org.grozeille.bigdata.user.repositories;

import org.grozeille.bigdata.configurations.stereotype.JpaRepository;
import org.grozeille.bigdata.user.model.UserWithProject;
import org.springframework.data.repository.CrudRepository;

@JpaRepository
public interface UserWithProjectRepository extends CrudRepository<UserWithProject, String> {
}
