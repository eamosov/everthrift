package com.knockchat.appserver.cluster;

import java.util.ArrayList;
import java.util.List;

import org.jgroups.Address;

import com.knockchat.appserver.thrift.cluster.NodeList;

public class NodeListModel extends NodeList {
	
	public List<Address> jGroupsAddresses;

	public NodeListModel() {
		
	}

	public NodeListModel(List<Address> jGroupsAddresses, List<String> hosts, List<Integer> ports, int hash) {
		super(hosts, ports, hash);
		this.jGroupsAddresses = jGroupsAddresses;
	}

	public NodeListModel(NodeList other) {
		super(other);		
	}

	public NodeListModel(NodeListModel other) {
		super(other);
		this.jGroupsAddresses = new ArrayList<Address>(other.jGroupsAddresses);
	}
	
}
