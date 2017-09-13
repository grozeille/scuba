package org.grozeille.bigdata.home.web;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

@RestController
@RequestMapping("/")
public class HomeResource {

    @RequestMapping("/")
    private void home(HttpServletResponse response) throws IOException {
        response.sendRedirect("/index.html");
    }
}
