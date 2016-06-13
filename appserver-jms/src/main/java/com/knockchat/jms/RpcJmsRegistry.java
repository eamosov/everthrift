package com.knockchat.jms;

import com.knockchat.appserver.controller.ThriftControllerRegistry;
import com.knockchat.appserver.transport.jms.RpcJms;

public class RpcJmsRegistry extends ThriftControllerRegistry{
	
	public RpcJmsRegistry() {
		super(RpcJms.class);
	}
	
}
