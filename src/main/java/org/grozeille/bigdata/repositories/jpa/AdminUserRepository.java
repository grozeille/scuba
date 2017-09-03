package org.grozeille.bigdata.repositories.jpa;

import org.grozeille.bigdata.resources.admin.model.AdminUser;
import org.springframework.data.repository.CrudRepository;

public interface AdminUserRepository extends CrudRepository<AdminUser, String> {
}
