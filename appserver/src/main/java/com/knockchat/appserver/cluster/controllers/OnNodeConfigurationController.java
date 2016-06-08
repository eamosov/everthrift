package com.knockchat.appserver.cluster.controllers;

import org.apache.thrift.TException;
import org.jgroups.Address;
import org.springframework.beans.factory.annotation.Autowired;

import com.knockchat.appserver.controller.ThriftController;
import com.knockchat.appserver.jgroups.RpcJGroups;
import com.knockchat.appserver.thrift.cluster.ClusterService;
import com.knockchat.appserver.thrift.cluster.ClusterService.onNodeConfiguration_args;
import com.knockchat.clustering.jgroups.ClusterThriftClientImpl;

@RpcJGroups
public class OnNodeConfigurationController extends ThriftController<ClusterService.onNodeConfiguration_args, Void> {

	@Autowired
	private ClusterThriftClientImpl client;
	
	@Override
	public void setup(onNodeConfiguration_args args) {
		this.noProfile = true;
	}

	@Override
	protected Void handle() throws TException {		
		client.setNode((Address)attributes.getAttribute("src"), args.getNode());
		return null;
	}

}
