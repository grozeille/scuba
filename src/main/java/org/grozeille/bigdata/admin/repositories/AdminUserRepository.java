package org.grozeille.bigdata.admin.repositories;

import org.grozeille.bigdata.configurations.stereotype.JpaRepository;
import org.grozeille.bigdata.admin.model.AdminUser;
import org.springframework.data.repository.CrudRepository;

@JpaRepository
public interface AdminUserRepository extends CrudRepository<AdminUser, String> {
}
