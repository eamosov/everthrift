package org.everthrift.appserver.jgroups;

import com.google.common.collect.Lists;
import org.everthrift.appserver.controller.ThriftControllerRegistry;
import org.everthrift.services.thrift.cluster.Node;
import org.jetbrains.annotations.NotNull;

import java.util.List;

public class RpcJGroupsRegistry extends ThriftControllerRegistry {

    public RpcJGroupsRegistry() {
        super(RpcJGroups.class);
    }

    @NotNull
    public Node getNodeConfiguration() {
        return new Node(Lists.newArrayList(getContollerNames()));
    }
}
