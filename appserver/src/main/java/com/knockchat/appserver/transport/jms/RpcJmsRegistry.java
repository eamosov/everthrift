package com.knockchat.appserver.transport.jms;

import java.util.ArrayList;

import com.knockchat.appserver.controller.ThriftControllerRegistry;
import com.knockchat.appserver.thrift.cluster.NodeAddress;
import com.knockchat.appserver.thrift.cluster.NodeControllers;

public class RpcJmsRegistry extends ThriftControllerRegistry{
	

	public RpcJmsRegistry() {
		super(RpcJms.class);
	}

	@Override
	public NodeControllers getNodeControllers(){
		final NodeControllers cc =  new NodeControllers(applicationContext.getEnvironment().getProperty("version"), new NodeAddress(null, 0), null);
		cc.setExternalControllers(new ArrayList<String>(getContollerNames()));		
		return cc;		
	}
	
}
