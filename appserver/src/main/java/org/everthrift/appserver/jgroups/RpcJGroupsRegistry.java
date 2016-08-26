package org.everthrift.appserver.jgroups;

import com.google.common.collect.Lists;
import org.everthrift.appserver.controller.ThriftControllerRegistry;
import org.everthrift.services.thrift.cluster.Node;

public class RpcJGroupsRegistry extends ThriftControllerRegistry {

    public RpcJGroupsRegistry() {
        super(RpcJGroups.class);
    }

    public Node getNodeConfiguration() {
        return new Node(Lists.newArrayList(getContollerNames()));
    }
}
