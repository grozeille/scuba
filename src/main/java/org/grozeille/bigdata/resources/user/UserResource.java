package org.grozeille.bigdata.resources.user;

import io.swagger.annotations.ApiParam;
import lombok.extern.slf4j.Slf4j;
import org.grozeille.bigdata.repositories.jpa.AdminUserRepository;
import org.grozeille.bigdata.repositories.jpa.ProjectRepository;
import org.grozeille.bigdata.repositories.jpa.UserRepository;
import org.grozeille.bigdata.repositories.jpa.UserWithProjectRepository;
import org.grozeille.bigdata.resources.admin.model.AdminUser;
import org.grozeille.bigdata.resources.project.model.Project;
import org.grozeille.bigdata.resources.project.model.ProjectUser;
import org.grozeille.bigdata.resources.user.model.SetLastProjectRequest;
import org.grozeille.bigdata.resources.user.model.User;
import org.grozeille.bigdata.resources.user.model.UserProject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.*;
import springfox.documentation.annotations.ApiIgnore;

import java.security.Principal;

@RestController
@Slf4j
@RequestMapping("/api/user")
public class UserResource {

    @Autowired
    private UserRepository userRepository;

    @Autowired
    private ProjectRepository projectRepository;

    @Autowired
    private UserWithProjectRepository userWithProjectRepository;

    @Autowired
    private AdminUserRepository adminUserRepository;

    @PreAuthorize("hasRole('ADMIN')")
    @RequestMapping(value = "", method = RequestMethod.GET)
    public Iterable<User> getAll() {
        return this.userRepository.findAll();
    }

    @RequestMapping(value = "/current",  method = RequestMethod.GET)
    public User user(@ApiIgnore @ApiParam(hidden = true) Principal principal) {
        return this.userRepository.findOne(principal.getName());
    }

    @RequestMapping(value = "/current/is-admin",  method = RequestMethod.GET)
    public Boolean isAdmin(@ApiIgnore @ApiParam(hidden = true) Principal principal) {
        AdminUser adminUser = adminUserRepository.findOne(principal.getName());
        return adminUser != null;
    }

    @RequestMapping(value = "/current/project",  method = RequestMethod.GET)
    public Iterable<UserProject> userProject(@ApiIgnore @ApiParam(hidden = true) Principal principal) {
        return this.userWithProjectRepository.findOne(principal.getName()).getProjects();
    }

    @RequestMapping(value = "/current/last-project",  method = RequestMethod.POST)
    public ResponseEntity<Void> lastProject(@ApiIgnore @ApiParam(hidden = true) Principal principal, @RequestBody SetLastProjectRequest request) {
        User user = this.userRepository.findOne(principal.getName());

        Project project = this.projectRepository.findOne(request.getProjectId());
        if(project == null) {
            return ResponseEntity.notFound().build();
        }

        user.setLastProject(request.getProjectId());
        this.userRepository.save(user);

        return ResponseEntity.ok().build();
    }
}
