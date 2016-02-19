package com.knockchat.appserver.transport.asynctcp;

import java.util.ArrayList;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;

import org.springframework.integration.ip.tcp.connection.AbstractServerConnectionFactory;

import com.knockchat.appserver.controller.ThriftControllerRegistry;
import com.knockchat.appserver.thrift.cluster.NodeAddress;
import com.knockchat.appserver.thrift.cluster.NodeControllers;
import com.knockchat.utils.NetUtils;

public class RpcAsyncTcpRegistry extends ThriftControllerRegistry{
	
	@Resource
	private AbstractServerConnectionFactory server;

	public RpcAsyncTcpRegistry() {
		super(RpcAsyncTcp.class);		
	}
	
	@PostConstruct
	private void logHostPort(){
		log.info("Async thrift tpc server on {}:{}", server.getLocalAddress(), server.getPort());		
	}

	@Override
	public NodeControllers getNodeControllers(){
		final NodeControllers cc =  new NodeControllers(applicationContext.getEnvironment().getProperty("version"), new NodeAddress(NetUtils.localToPublic(server.getLocalAddress()), server.getPort()), null);
		cc.setExternalControllers(new ArrayList<String>(getContollerNames()));		
		return cc;		
	}
	
}
