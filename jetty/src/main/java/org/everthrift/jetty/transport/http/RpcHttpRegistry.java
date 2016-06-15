package org.everthrift.jetty.transport.http;

import org.everthrift.appserver.controller.ThriftControllerRegistry;
import org.everthrift.appserver.transport.http.RpcHttp;
import org.everthrift.jetty.JettyServer;
import org.springframework.beans.factory.annotation.Autowired;

public class RpcHttpRegistry extends ThriftControllerRegistry{
	
	@Autowired
	private JettyServer jettyServer;

	public RpcHttpRegistry() {
		super(RpcHttp.class);
	}	
}
