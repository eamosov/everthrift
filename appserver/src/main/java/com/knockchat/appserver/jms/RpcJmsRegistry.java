package com.knockchat.appserver.jms;

import com.knockchat.appserver.controller.ThriftControllerRegistry;

public class RpcJmsRegistry extends ThriftControllerRegistry{
	
	public RpcJmsRegistry() {
		super(RpcJms.class);
	}
	
}
