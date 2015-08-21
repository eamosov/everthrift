package com.knockchat.node.controllers;

import org.apache.thrift.TException;
import org.springframework.beans.factory.annotation.Autowired;

import com.knockchat.appserver.controller.ThriftController;
import com.knockchat.appserver.thrift.cluster.ClusterException;
import com.knockchat.appserver.thrift.cluster.ClusterService.getClusterConfigurationJSON_args;
import com.knockchat.appserver.transport.asynctcp.RpcAsyncTcp;
import com.knockchat.appserver.transport.tcp.RpcSyncTcp;
import com.knockchat.node.ScanConfigurationTask;

@RpcSyncTcp
@RpcAsyncTcp
public class GetClusterConfigurationJSONController extends ThriftController<getClusterConfigurationJSON_args, String> {

	@Autowired
	private ScanConfigurationTask scanConfigurationTask;
	
	@Override
	public void setup(getClusterConfigurationJSON_args args) {
		
	}

	@Override
	protected String handle() throws TException {
		
		this.loadLazyRelations = false;
		
		final String l =  scanConfigurationTask.getConfigurationJSON();
		if (l==null)
			throw new ClusterException();
		
		return l;
	}

}
