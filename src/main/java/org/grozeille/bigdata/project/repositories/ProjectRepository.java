package org.grozeille.bigdata.project.repositories;

import org.grozeille.bigdata.configurations.stereotype.JpaRepository;
import org.grozeille.bigdata.project.model.Project;
import org.springframework.data.repository.CrudRepository;

@JpaRepository
public interface ProjectRepository extends CrudRepository<Project, String> {
}
