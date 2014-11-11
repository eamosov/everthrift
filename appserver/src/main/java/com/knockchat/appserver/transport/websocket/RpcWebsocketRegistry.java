package com.knockchat.appserver.transport.websocket;

import java.util.ArrayList;

import org.springframework.stereotype.Component;

import com.knockchat.appserver.AppserverApplication;
import com.knockchat.appserver.controller.ThriftControllerRegistry;
import com.knockchat.appserver.thrift.cluster.NodeControllers;

@Component
public class RpcWebsocketRegistry extends ThriftControllerRegistry{

	public RpcWebsocketRegistry() {
		super(RpcWebsocket.class);
	}
	
	@Override
	public NodeControllers getNodeControllers(){
		final NodeControllers cc =  new NodeControllers(applicationContext.getEnvironment().getProperty("version"), AppserverApplication.INSTANCE.jettyAddress, null);		
		cc.setExternalControllers(new ArrayList<String>(getContollerNames()));
		return cc;
	}
	
}
