package com.knockchat.appserver.transport.websocket;

import java.util.ArrayList;

import org.springframework.beans.factory.annotation.Autowired;

import com.knockchat.appserver.JettyServer;
import com.knockchat.appserver.controller.ThriftControllerRegistry;
import com.knockchat.appserver.thrift.cluster.NodeControllers;

public class RpcWebsocketRegistry extends ThriftControllerRegistry{

	@Autowired
	private JettyServer jettyServer;

	public RpcWebsocketRegistry() {
		super(RpcWebsocket.class);
	}
	
	@Override
	public NodeControllers getNodeControllers(){
		final NodeControllers cc =  new NodeControllers(applicationContext.getEnvironment().getProperty("version"), jettyServer.getNodeAddress(), null);		
		cc.setExternalControllers(new ArrayList<String>(getContollerNames()));
		return cc;
	}
	
}
