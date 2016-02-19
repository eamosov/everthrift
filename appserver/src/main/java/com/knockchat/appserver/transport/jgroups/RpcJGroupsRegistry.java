package com.knockchat.appserver.transport.jgroups;

import java.util.ArrayList;

import com.knockchat.appserver.controller.ThriftControllerRegistry;
import com.knockchat.appserver.thrift.cluster.NodeAddress;
import com.knockchat.appserver.thrift.cluster.NodeControllers;

public class RpcJGroupsRegistry extends ThriftControllerRegistry{
	
	public RpcJGroupsRegistry() {
		super(RpcJGroups.class);
	}

	@Override
	public NodeControllers getNodeControllers(){
		final NodeControllers cc =  new NodeControllers(applicationContext.getEnvironment().getProperty("version"), new NodeAddress(), null);
		cc.setExternalControllers(new ArrayList<String>(getContollerNames()));		
		return cc;		
	}
	
}
