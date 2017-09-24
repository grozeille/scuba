package fr.grozeille.scuba.project.repositories;

import fr.grozeille.scuba.configurations.stereotype.JpaRepository;
import fr.grozeille.scuba.project.model.Project;
import org.springframework.data.repository.CrudRepository;

@JpaRepository
public interface ProjectRepository extends CrudRepository<Project, String> {
}
