package org.everthrift.jetty.transport.websocket;

import org.everthrift.appserver.controller.ThriftControllerRegistry;
import org.everthrift.appserver.transport.websocket.RpcWebsocket;
import org.everthrift.jetty.JettyServer;
import org.springframework.beans.factory.annotation.Autowired;

public class RpcWebsocketRegistry extends ThriftControllerRegistry{

	@Autowired
	private JettyServer jettyServer;

	public RpcWebsocketRegistry() {
		super(RpcWebsocket.class);
	}
	
}
