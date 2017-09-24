package fr.grozeille.scuba.user.repositories;

import fr.grozeille.scuba.configurations.stereotype.JpaRepository;
import fr.grozeille.scuba.user.model.User;
import org.springframework.data.repository.CrudRepository;

@JpaRepository
public interface UserRepository extends CrudRepository<User, String> {
}
