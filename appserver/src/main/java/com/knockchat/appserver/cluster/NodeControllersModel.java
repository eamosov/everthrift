package com.knockchat.appserver.cluster;

import org.jgroups.Address;

import com.knockchat.appserver.thrift.cluster.NodeControllers;

public class NodeControllersModel extends NodeControllers {
	
	public Address jGroupsAddress;

	public NodeControllersModel() {
	}

	public NodeControllersModel(NodeControllers other) {
		super(other);
	}

	public NodeControllersModel(NodeControllersModel other) {
		super(other);
		this.jGroupsAddress = other.jGroupsAddress;
	}

	public NodeControllersModel(NodeControllers other, Address jGroupsAddress) {
		super(other);
		this.jGroupsAddress = jGroupsAddress;
	}
	
}
