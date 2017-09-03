package org.grozeille.bigdata.resources.admin;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import lombok.extern.slf4j.Slf4j;
import org.grozeille.bigdata.ClusterConfiguration;
import org.grozeille.bigdata.repositories.jpa.AdminUserRepository;
import org.grozeille.bigdata.repositories.solr.DataSetRepository;
import org.grozeille.bigdata.resources.admin.model.AdminUser;
import org.grozeille.bigdata.resources.dataset.model.DataSet;
import org.grozeille.bigdata.resources.hive.model.HiveTable;
import org.grozeille.bigdata.services.CatalogService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.*;

import javax.annotation.security.RolesAllowed;
import java.io.IOException;

@RestController
@Slf4j
@RequestMapping("/api/admin")
public class AdminResource {

    @Autowired
    private AdminUserRepository adminUserRepository;

    @Autowired
    private ClusterConfiguration clusterConfiguration;

    @PreAuthorize("hasRole('ADMIN')")
    @RequestMapping(value = "", method = RequestMethod.GET)
    public Iterable<AdminUser> adminUsers() {
        return this.adminUserRepository.findAll();
    }

    @PreAuthorize("hasRole('ADMIN')")
    @RequestMapping(value = "/{login}", method = RequestMethod.PUT)
    public void add(@PathVariable("login") String login) {
        this.adminUserRepository.save(new AdminUser(login));
    }

    @RequestMapping(value = "/setup/{login}", method = RequestMethod.PUT)
    public ResponseEntity<String> addWithAdminToken(@PathVariable("login") String login, @RequestParam("adminToken") String adminToken) {

        if(clusterConfiguration.getAdminToken().contentEquals(adminToken)) {
            this.adminUserRepository.save(new AdminUser(login));
            return ResponseEntity.ok("New admin created");
        }
        else {
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).body("unvalid adminToken");
        }
    }

    @PreAuthorize("hasRole('ADMIN')")
    @RequestMapping(value = "/{login}", method = RequestMethod.DELETE)
    public void delete(@PathVariable("login") String login) {
        this.adminUserRepository.delete(new AdminUser(login));
    }
}
