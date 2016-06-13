package com.knockchat.jetty.controllers.api;

import java.io.IOException;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

import com.knockchat.jetty.transport.http.RpcHttpRegistry;
import com.knockchat.utils.thrift.ThriftFormatter;

@Controller
public class ApiController {
	
	@Autowired
	private RpcHttpRegistry rpcHttpRegistry;

	@RequestMapping(value="/api", method = RequestMethod.GET, produces="text/html")
    public void getServices(HttpServletRequest request, HttpServletResponse response) throws IOException {
		
		final byte[] services = new ThriftFormatter("/api/").formatServices(rpcHttpRegistry.getControllers().values()).getBytes();
		
		response.setContentType("text/html");
		response.setContentLength(services.length);
		response.getOutputStream().write(services);
		response.flushBuffer();
	}
}
