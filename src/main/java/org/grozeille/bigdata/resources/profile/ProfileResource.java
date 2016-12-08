package org.grozeille.bigdata.resources.profile;

import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.security.Principal;

@RestController
@Slf4j
@RequestMapping("/api/profile")
public class ProfileResource {

    @RequestMapping(value = "/user",  method = RequestMethod.GET)
    public Principal user(Principal principal) {
        return principal;
    }
}
