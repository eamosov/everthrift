package com.knockchat.appserver.transport.tcp;

import java.util.ArrayList;

import org.springframework.stereotype.Component;

import com.knockchat.appserver.controller.ThriftControllerRegistry;
import com.knockchat.appserver.thrift.cluster.NodeAddress;
import com.knockchat.appserver.thrift.cluster.NodeControllers;

@Component
public class RpcSyncTcpRegistry extends ThriftControllerRegistry{

	public RpcSyncTcpRegistry() {
		super(RpcSyncTcp.class);
	}
	
	@Override
	public NodeControllers getNodeControllers(){
		final ThriftServer t = applicationContext.getBean(ThriftServer.class);		
		final NodeControllers cc =  new NodeControllers(applicationContext.getEnvironment().getProperty("version"), new NodeAddress(t.getLocalAddress(), t.getPort()), null);		
		cc.setExternalControllers(new ArrayList<String>(getContollerNames()));
		return cc;
	}
	
}
