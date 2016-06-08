package com.knockchat.appserver.jgroups;

import com.google.common.collect.Lists;
import com.knockchat.appserver.controller.ThriftControllerRegistry;
import com.knockchat.appserver.thrift.cluster.Node;

public class RpcJGroupsRegistry extends ThriftControllerRegistry{
	
	public RpcJGroupsRegistry() {
		super(RpcJGroups.class);
	}
		
	public Node getNodeConfiguration(){
		return new Node(Lists.newArrayList(getContollerNames()));		
	}
}
