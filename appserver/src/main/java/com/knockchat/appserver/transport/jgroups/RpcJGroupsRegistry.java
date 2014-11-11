package com.knockchat.appserver.transport.jgroups;

import java.util.ArrayList;

import javax.annotation.Resource;

import org.jgroups.JChannel;
import org.springframework.stereotype.Component;

import com.knockchat.appserver.controller.ThriftControllerRegistry;
import com.knockchat.appserver.thrift.cluster.NodeAddress;
import com.knockchat.appserver.thrift.cluster.NodeControllers;

@Component
public class RpcJGroupsRegistry extends ThriftControllerRegistry{
	
	@Resource
	private JChannel yocluster;

	public RpcJGroupsRegistry() {
		super(RpcJGroups.class);
	}

	@Override
	public NodeControllers getNodeControllers(){
		final NodeControllers cc =  new NodeControllers(applicationContext.getEnvironment().getProperty("version"), new NodeAddress(yocluster.getAddressAsString(), Integer.parseInt(System.getenv("jgroups.udp.mcast_port"))), null);
		cc.setExternalControllers(new ArrayList<String>(getContollerNames()));		
		return cc;		
	}
	
}
