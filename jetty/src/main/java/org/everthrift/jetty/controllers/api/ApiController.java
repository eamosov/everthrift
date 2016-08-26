package org.everthrift.jetty.controllers.api;

import org.everthrift.appserver.utils.thrift.ThriftFormatter;
import org.everthrift.jetty.transport.http.RpcHttpRegistry;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

@Controller
public class ApiController {

    @Autowired
    private RpcHttpRegistry rpcHttpRegistry;

    @RequestMapping(value = "/api", method = RequestMethod.GET, produces = "text/html")
    public void getServices(HttpServletRequest request, HttpServletResponse response) throws IOException {

        final byte[] services = new ThriftFormatter("/api/").formatServices(rpcHttpRegistry.getControllers().values())
                                                            .getBytes(StandardCharsets.UTF_8);

        response.setContentType("text/html");
        response.setContentLength(services.length);
        response.getOutputStream().write(services);
        response.flushBuffer();
    }
}
