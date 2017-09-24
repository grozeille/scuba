package fr.grozeille.scuba.user.repositories;

import fr.grozeille.scuba.configurations.stereotype.JpaRepository;
import fr.grozeille.scuba.user.model.UserWithProject;
import org.springframework.data.repository.CrudRepository;

@JpaRepository
public interface UserWithProjectRepository extends CrudRepository<UserWithProject, String> {
}
