package fr.grozeille.scuba.configurations;

import fr.grozeille.scuba.user.model.User;
import fr.grozeille.scuba.user.repositories.UserRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationListener;
import org.springframework.security.authentication.event.InteractiveAuthenticationSuccessEvent;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.time.LocalDateTime;
import java.util.TimeZone;

@Service
@Slf4j
public class InternalAuthenticationListener implements ApplicationListener<InteractiveAuthenticationSuccessEvent> {

    @Autowired
    private UserRepository userRepository;

    @Override
    public void onApplicationEvent(final InteractiveAuthenticationSuccessEvent event) {
        log.info("Success login form " + event.getAuthentication().getName());

        User user = this.userRepository.findOne(event.getAuthentication().getName());
        if(user == null) {
            user = new User();
            user.setLogin(event.getAuthentication().getName());
        }

        LocalDateTime triggerTime =
                LocalDateTime.ofInstant(Instant.ofEpochMilli(event.getTimestamp()), TimeZone.getDefault().toZoneId());

        user.setLastLogin(triggerTime);

        // keep a trace of the authentication
        userRepository.save(user);
    }

}
