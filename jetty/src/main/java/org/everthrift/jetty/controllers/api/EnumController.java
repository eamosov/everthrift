package org.everthrift.jetty.controllers.api;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.everthrift.appserver.utils.thrift.ThriftFormatter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

@Controller
public class EnumController {

    @Autowired
    private ApplicationContext context;

    @RequestMapping(value="/api/enum/{enumClassName}", method = RequestMethod.GET, produces="text/html")
    public void getStruct(@PathVariable("enumClassName") String enumClassName, HttpServletRequest request, HttpServletResponse response) throws IOException {

        final String tbaseRoot = context.getEnvironment().getProperty("tbase.root");

        if (enumClassName==null || !enumClassName.startsWith(tbaseRoot)){
            response.setStatus(403);
            response.getOutputStream().write(("Class '" +  enumClassName + "' not allowed").getBytes(StandardCharsets.UTF_8));
            response.flushBuffer();
            return;
        }

        try {
            final Class cls = Class.forName(enumClassName, false, EnumController.class.getClassLoader());

            final byte[] services = new ThriftFormatter("/api/").formatTEnum(cls).getBytes(StandardCharsets.UTF_8);

            response.setContentType("text/html");
            response.setContentLength(services.length);
            response.getOutputStream().write(services);
            response.flushBuffer();

        } catch (ClassNotFoundException e) {
            response.setStatus(404);
            response.getOutputStream().write(("Class '" +  enumClassName + "' not found").getBytes(StandardCharsets.UTF_8));
            response.flushBuffer();
            return;
        }

    }

}
