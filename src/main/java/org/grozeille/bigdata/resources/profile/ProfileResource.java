package org.grozeille.bigdata.resources.profile;

import io.swagger.annotations.ApiParam;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import springfox.documentation.annotations.ApiIgnore;

import java.security.Principal;

@RestController
@Slf4j
@RequestMapping("/api/profile")
public class ProfileResource {

    @RequestMapping(value = "/user",  method = RequestMethod.GET)
    public Principal user(@ApiIgnore @ApiParam(hidden = true) Principal principal) {
        return principal;
    }
}
