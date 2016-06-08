package com.knockchat.appserver.transport.tcp;

import com.knockchat.appserver.controller.ThriftControllerRegistry;

public class RpcSyncTcpRegistry extends ThriftControllerRegistry{

	public RpcSyncTcpRegistry() {
		super(RpcSyncTcp.class);
	}
	
}
