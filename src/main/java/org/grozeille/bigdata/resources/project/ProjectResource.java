package org.grozeille.bigdata.resources.project;

import io.swagger.annotations.ApiParam;
import lombok.extern.slf4j.Slf4j;
import org.grozeille.bigdata.ClusterConfiguration;
import org.grozeille.bigdata.repositories.jpa.ProjectRepository;
import org.grozeille.bigdata.resources.admin.model.AdminWithTokenRequest;
import org.grozeille.bigdata.resources.project.model.Project;
import org.grozeille.bigdata.resources.project.model.NewProjectRequest;
import org.grozeille.bigdata.resources.project.model.NewProjectResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.*;
import springfox.documentation.annotations.ApiIgnore;

import java.security.Principal;
import java.util.UUID;

@RestController
@Slf4j
@PreAuthorize("hasRole('ADMIN')")
@RequestMapping("/api/project")
public class ProjectResource {

    @Autowired
    private ProjectRepository projectRepository;

    @RequestMapping(value = "", method = RequestMethod.GET)
    public Iterable<Project> get() {
        return this.projectRepository.findAll();
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
