package org.grozeille.bigdata.project.web;

import lombok.extern.slf4j.Slf4j;
import org.grozeille.bigdata.project.repositories.ProjectRepository;
import org.grozeille.bigdata.project.model.Project;
import org.grozeille.bigdata.project.web.dto.NewProjectRequest;
import org.grozeille.bigdata.project.web.dto.NewProjectResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.*;

import java.util.UUID;

@RestController
@Slf4j
@PreAuthorize("hasRole('ADMIN')")
@RequestMapping("/api/project")
public class ProjectResource {

    @Autowired
    private ProjectRepository projectRepository;

    @RequestMapping(value = "", method = RequestMethod.GET)
    public Iterable<Project> getAll() {
        return this.projectRepository.findAll();
    }

    @RequestMapping(value = "/{id}", method = RequestMethod.GET)
    public Project get(@PathVariable("id") String id) {
        return this.projectRepository.findOne(id);
    }

    @RequestMapping(value = "/{id}", method = RequestMethod.PUT)
    public ResponseEntity<Void> put(@PathVariable("id") String id, @RequestBody Project project) {
        this.projectRepository.save(project);

        return ResponseEntity.ok().build();
    }

    @RequestMapping(value = "", method = RequestMethod.POST)
    public NewProjectResponse add(@RequestBody NewProjectRequest request) {
        Project project = new Project();
        project.setId(UUID.randomUUID().toString());
        project.setName(request.getName());
        project.setHiveDatabase(request.getHiveDatabase());
        project.setHdfsWorkingDirectory(request.getHdfsWorkingDirectory());
        project = this.projectRepository.save(project);

        return new NewProjectResponse(project.getId());
    }

    @RequestMapping(value = "/{id}", method = RequestMethod.DELETE)
    public void delete(@PathVariable("id") String id) {
        this.projectRepository.delete(id);
    }

}
