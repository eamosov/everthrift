package com.knockchat.appserver.cluster.controllers;

import org.apache.thrift.TException;

import com.knockchat.appserver.controller.ThriftController;
import com.knockchat.appserver.jgroups.RpcJGroups;
import com.knockchat.appserver.jgroups.RpcJGroupsRegistry;
import com.knockchat.appserver.thrift.cluster.ClusterService;
import com.knockchat.appserver.thrift.cluster.ClusterService.getNodeConfiguration_args;
import com.knockchat.appserver.thrift.cluster.Node;

@RpcJGroups
public class GetNodeConfigurationController extends ThriftController<ClusterService.getNodeConfiguration_args, Node> {

	@Override
	public void setup(getNodeConfiguration_args args) {
		this.noProfile = true;
	}
	
	@Override
	protected Node handle() throws TException {		
		final RpcJGroupsRegistry r = context.getBean(RpcJGroupsRegistry.class);		
		return r == null ? new Node() : r.getNodeConfiguration();
	}	
}
