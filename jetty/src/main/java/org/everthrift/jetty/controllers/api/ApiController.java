package org.everthrift.jetty.controllers.api;

import org.everthrift.clustering.thrift.ThriftControllerDiscovery;
import org.everthrift.appserver.transport.http.RpcHttp;
import org.everthrift.appserver.utils.thrift.ThriftFormatter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static java.util.stream.Collectors.toList;

@Controller
public class ApiController {

    @Autowired
    private ThriftControllerDiscovery controllers;

    @RequestMapping(value = "/api", method = RequestMethod.GET, produces = "text/html")
    public void getServices(HttpServletRequest request, HttpServletResponse response) throws IOException {

        final byte[] services = new ThriftFormatter("/api/").formatServices(controllers.getLocal(RpcHttp.class.getSimpleName())
                                                                                       .stream()
                                                                                       .map(e -> e.thriftMethodEntry)
                                                                                       .collect(toList()))
                                                            .getBytes(StandardCharsets.UTF_8);

        response.setContentType("text/html");
        response.setContentLength(services.length);
        response.getOutputStream().write(services);
        response.flushBuffer();
    }
}
