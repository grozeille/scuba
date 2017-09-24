package fr.grozeille.scuba.admin.repositories;

import fr.grozeille.scuba.admin.model.AdminUser;
import fr.grozeille.scuba.configurations.stereotype.JpaRepository;
import org.springframework.data.repository.CrudRepository;

@JpaRepository
public interface AdminUserRepository extends CrudRepository<AdminUser, String> {
}
