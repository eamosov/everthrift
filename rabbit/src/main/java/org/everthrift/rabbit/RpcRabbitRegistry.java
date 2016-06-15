package org.everthrift.rabbit;

import org.everthrift.appserver.controller.ThriftControllerRegistry;

public class RpcRabbitRegistry extends ThriftControllerRegistry{
	
	public RpcRabbitRegistry() {
		super(RpcRabbit.class);
	}
	
}
