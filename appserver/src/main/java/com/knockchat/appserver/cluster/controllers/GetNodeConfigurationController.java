package com.knockchat.appserver.cluster.controllers;

import java.util.ArrayList;
import java.util.List;

import org.apache.thrift.TException;
import org.springframework.jmx.support.ConnectorServerFactoryBean;

import com.knockchat.appserver.AppserverApplication;
import com.knockchat.appserver.controller.ThriftController;
import com.knockchat.appserver.controller.ThriftControllerRegistry;
import com.knockchat.appserver.thrift.cluster.ClusterService;
import com.knockchat.appserver.thrift.cluster.ClusterService.getNodeConfiguration_args;
import com.knockchat.appserver.thrift.cluster.Node;
import com.knockchat.appserver.thrift.cluster.NodeAddress;
import com.knockchat.appserver.thrift.cluster.NodeControllers;
import com.knockchat.appserver.transport.asynctcp.RpcAsyncTcp;
import com.knockchat.appserver.transport.jgroups.RpcJGroups;
import com.knockchat.appserver.transport.tcp.RpcSyncTcp;

@RpcJGroups
@RpcSyncTcp
public class GetNodeConfigurationController extends ThriftController<ClusterService.getNodeConfiguration_args, Node> {

	@Override
	public void setup(getNodeConfiguration_args args) {
		this.noProfile = true;
	}
	
	@Override
	protected Node handle() throws TException {
				
		final List<NodeControllers> nc = new ArrayList<NodeControllers>();
		
		for (ThriftControllerRegistry r: context.getBeansOfType(ThriftControllerRegistry.class).values()){
			
			if (r.getType().equals(RpcSyncTcp.class) || r.getType().equals(RpcAsyncTcp.class)){
				final NodeControllers i = r.getNodeControllers();
				if (i!=null)
					nc.add(i);				
			}
		}
		
		this.loadLazyRelations = false;
		
		final ConnectorServerFactoryBean cs = context.getBean(ConnectorServerFactoryBean.class);
		
		return new Node(nc,
				AppserverApplication.INSTANCE.env.getProperty("node.name"), 				
				(cs !=null && cs.getObject().getAddress() !=null) ? new NodeAddress(cs.getObject().getAddress().getHost(), cs.getObject().getAddress().getPort()) : null,
				AppserverApplication.INSTANCE.jettyAddress);
	}	
}
