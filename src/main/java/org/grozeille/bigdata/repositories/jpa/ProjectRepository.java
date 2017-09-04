package org.grozeille.bigdata.repositories.jpa;

import org.grozeille.bigdata.resources.project.model.Project;
import org.springframework.data.repository.CrudRepository;

public interface ProjectRepository extends CrudRepository<Project, String> {
}
