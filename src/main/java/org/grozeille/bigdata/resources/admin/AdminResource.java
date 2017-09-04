package org.grozeille.bigdata.resources.admin;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import io.swagger.annotations.ApiParam;
import lombok.extern.slf4j.Slf4j;
import org.grozeille.bigdata.ClusterConfiguration;
import org.grozeille.bigdata.repositories.jpa.AdminUserRepository;
import org.grozeille.bigdata.repositories.jpa.UserRepository;
import org.grozeille.bigdata.repositories.solr.DataSetRepository;
import org.grozeille.bigdata.resources.admin.model.AdminUser;
import org.grozeille.bigdata.resources.admin.model.AdminWithTokenRequest;
import org.grozeille.bigdata.resources.admin.model.AdminWithTokenResponse;
import org.grozeille.bigdata.resources.dataset.model.DataSet;
import org.grozeille.bigdata.resources.hive.model.HiveTable;
import org.grozeille.bigdata.resources.user.model.User;
import org.grozeille.bigdata.services.CatalogService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.*;
import springfox.documentation.annotations.ApiIgnore;

import javax.annotation.PostConstruct;
import javax.annotation.security.RolesAllowed;
import java.io.IOException;
import java.security.Principal;

@RestController
@Slf4j
@RequestMapping("/api/admin")
public class AdminResource {

    @Autowired
    private AdminUserRepository adminUserRepository;

    @Autowired
    private UserRepository userRepository;

    @Autowired
    private ClusterConfiguration clusterConfiguration;

    @PreAuthorize("hasRole('ADMIN')")
    @RequestMapping(value = "", method = RequestMethod.GET)
    public Iterable<AdminUser> adminUsers() {
        return this.adminUserRepository.findAll();
    }

    @PreAuthorize("hasRole('ADMIN')")
    @RequestMapping(value = "/{login}", method = RequestMethod.PUT)
    public ResponseEntity<Void> add(@PathVariable("login") String login) {

        User user = userRepository.findOne(login);
        if(user == null) {
            return ResponseEntity.notFound().build();
        }

        this.adminUserRepository.save(new AdminUser(login));

        return ResponseEntity.ok().build();
    }

    @RequestMapping(value = "/current-user", method = RequestMethod.POST)
    public ResponseEntity<AdminWithTokenResponse> addWithAdminToken(
            @RequestBody AdminWithTokenRequest request,
            @ApiIgnore @ApiParam(hidden = true)  Principal principal) {

        if(clusterConfiguration.getAdminToken().contentEquals(request.getAdminToken())) {
            this.adminUserRepository.save(new AdminUser(principal.getName()));
            log.info("New admin created using adminToken: "+principal.getName());
            return ResponseEntity.ok(new AdminWithTokenResponse(principal.getName()));
        }
        else {
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).body(new AdminWithTokenResponse());
        }
    }

    @PreAuthorize("hasRole('ADMIN')")
    @RequestMapping(value = "/{login}", method = RequestMethod.DELETE)
    public ResponseEntity<Void> delete(@PathVariable("login") String login) {

        AdminUser admin = this.adminUserRepository.findOne(login);
        if(admin == null) {
            return ResponseEntity.notFound().build();
        }

        this.adminUserRepository.delete(login);

        return ResponseEntity.ok().build();
    }

}
