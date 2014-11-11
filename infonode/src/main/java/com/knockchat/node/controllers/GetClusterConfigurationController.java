package com.knockchat.node.controllers;

import org.apache.thrift.TException;
import org.springframework.beans.factory.annotation.Autowired;

import com.knockchat.appserver.controller.ThriftController;
import com.knockchat.appserver.model.LazyLoadManager;
import com.knockchat.appserver.thrift.cluster.ClusterConfiguration;
import com.knockchat.appserver.thrift.cluster.ClusterException;
import com.knockchat.appserver.thrift.cluster.ClusterService;
import com.knockchat.appserver.thrift.cluster.ClusterService.getClusterConfiguration_args;
import com.knockchat.appserver.transport.asynctcp.RpcAsyncTcp;
import com.knockchat.appserver.transport.tcp.RpcSyncTcp;
import com.knockchat.node.ScanConfigurationTask;

@RpcSyncTcp
@RpcAsyncTcp
public class GetClusterConfigurationController extends ThriftController<ClusterService.getClusterConfiguration_args, ClusterConfiguration> {

	@Autowired
	private ScanConfigurationTask scanConfigurationTask;
	
	@Override
	public void setup(getClusterConfiguration_args args) {
		
	}

	@Override
	protected ClusterConfiguration handle() throws TException {
		
		LazyLoadManager.disable();
		
		final ClusterConfiguration l =  scanConfigurationTask.getConfiguration();
		if (l==null)
			throw new ClusterException();
		
		return l;
	}

}
