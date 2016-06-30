package org.everthrift.appserver.jgroups;

import org.everthrift.appserver.controller.ThriftControllerRegistry;
import org.everthrift.services.thrift.cluster.Node;

import com.google.common.collect.Lists;

public class RpcJGroupsRegistry extends ThriftControllerRegistry{

    public RpcJGroupsRegistry() {
        super(RpcJGroups.class);
    }

    public Node getNodeConfiguration(){
        return new Node(Lists.newArrayList(getContollerNames()));
    }
}
