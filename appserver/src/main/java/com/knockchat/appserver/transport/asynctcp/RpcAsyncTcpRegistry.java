package com.knockchat.appserver.transport.asynctcp;

import java.util.ArrayList;

import org.springframework.integration.ip.tcp.connection.AbstractServerConnectionFactory;
import org.springframework.stereotype.Component;

import com.knockchat.appserver.controller.ThriftControllerRegistry;
import com.knockchat.appserver.thrift.cluster.NodeAddress;
import com.knockchat.appserver.thrift.cluster.NodeControllers;
import com.knockchat.utils.NetUtils;

@Component
public class RpcAsyncTcpRegistry extends ThriftControllerRegistry{

	public RpcAsyncTcpRegistry() {
		super(RpcAsyncTcp.class);
	}

	@Override
	public NodeControllers getNodeControllers(){
		final AbstractServerConnectionFactory o = applicationContext.getBean("server", AbstractServerConnectionFactory.class);		
		final NodeControllers cc =  new NodeControllers(applicationContext.getEnvironment().getProperty("version"), new NodeAddress(NetUtils.localToPublic(o.getLocalAddress()), o.getPort()), null);
		cc.setExternalControllers(new ArrayList<String>(getContollerNames()));		
		return cc;		
	}
	
}
