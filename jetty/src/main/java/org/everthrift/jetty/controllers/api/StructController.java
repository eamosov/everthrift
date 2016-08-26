package org.everthrift.jetty.controllers.api;

import org.everthrift.appserver.utils.thrift.ThriftFormatter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

@Controller
public class StructController {

    @Autowired
    private ApplicationContext context;

    @RequestMapping(value = "/api/struct/{structClassName}", method = RequestMethod.GET, produces = "text/html")
    public void getStruct(@PathVariable("structClassName") String structClassName, HttpServletRequest request,
                          HttpServletResponse response) throws IOException {

        final String tbaseRoot = context.getEnvironment().getProperty("tbase.root");

        if (structClassName == null || !structClassName.startsWith(tbaseRoot)) {
            response.setStatus(403);
            response.getOutputStream()
                    .write(("Class '" + structClassName + "' not allowed").getBytes(StandardCharsets.UTF_8));
            response.flushBuffer();
            return;
        }

        try {
            final Class cls = Class.forName(structClassName, false, StructController.class.getClassLoader());

            final byte[] services = new ThriftFormatter("/api/").formatClass(cls).getBytes(StandardCharsets.UTF_8);

            response.setContentType("text/html");
            response.setContentLength(services.length);
            response.getOutputStream().write(services);
            response.flushBuffer();

        } catch (ClassNotFoundException e) {
            response.setStatus(404);
            response.getOutputStream()
                    .write(("Class '" + structClassName + "' not found").getBytes(StandardCharsets.UTF_8));
            response.flushBuffer();
            return;
        }

    }

}
