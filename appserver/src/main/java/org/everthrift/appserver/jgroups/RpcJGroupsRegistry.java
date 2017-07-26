package org.everthrift.appserver.jgroups;

import com.google.common.collect.Lists;
import org.everthrift.appserver.controller.ThriftControllerRegistry;
import org.everthrift.services.thrift.cluster.Node;

import java.util.List;

public class RpcJGroupsRegistry extends ThriftControllerRegistry {

    public RpcJGroupsRegistry(final List<String> basePaths) {
        super(RpcJGroups.class, basePaths);
    }

    public Node getNodeConfiguration() {
        return new Node(Lists.newArrayList(getContollerNames()));
    }
}
