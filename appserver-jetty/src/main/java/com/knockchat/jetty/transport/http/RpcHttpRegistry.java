package com.knockchat.jetty.transport.http;

import org.springframework.beans.factory.annotation.Autowired;

import com.knockchat.appserver.controller.ThriftControllerRegistry;
import com.knockchat.appserver.transport.http.RpcHttp;
import com.knockchat.jetty.JettyServer;

public class RpcHttpRegistry extends ThriftControllerRegistry{
	
	@Autowired
	private JettyServer jettyServer;

	public RpcHttpRegistry() {
		super(RpcHttp.class);
	}	
}
