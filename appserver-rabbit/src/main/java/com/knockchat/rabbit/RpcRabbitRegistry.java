package com.knockchat.rabbit;

import com.knockchat.appserver.controller.ThriftControllerRegistry;

public class RpcRabbitRegistry extends ThriftControllerRegistry{
	
	public RpcRabbitRegistry() {
		super(RpcRabbit.class);
	}
	
}
