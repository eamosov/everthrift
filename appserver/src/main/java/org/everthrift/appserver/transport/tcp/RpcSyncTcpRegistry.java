package org.everthrift.appserver.transport.tcp;

import org.everthrift.appserver.controller.ThriftControllerRegistry;

public class RpcSyncTcpRegistry extends ThriftControllerRegistry{

	public RpcSyncTcpRegistry() {
		super(RpcSyncTcp.class);
	}
	
}
