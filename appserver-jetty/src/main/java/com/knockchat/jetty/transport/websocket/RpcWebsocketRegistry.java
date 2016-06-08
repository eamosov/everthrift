package com.knockchat.jetty.transport.websocket;

import org.springframework.beans.factory.annotation.Autowired;

import com.knockchat.appserver.controller.ThriftControllerRegistry;
import com.knockchat.appserver.transport.websocket.RpcWebsocket;
import com.knockchat.jetty.JettyServer;

public class RpcWebsocketRegistry extends ThriftControllerRegistry{

	@Autowired
	private JettyServer jettyServer;

	public RpcWebsocketRegistry() {
		super(RpcWebsocket.class);
	}
	
}
