package org.everthrift.appserver.cluster.controllers;

import org.apache.thrift.TException;
import org.everthrift.appserver.controller.ThriftController;
import org.everthrift.appserver.jgroups.RpcJGroups;
import org.everthrift.appserver.jgroups.RpcJGroupsRegistry;
import org.everthrift.services.thrift.cluster.ClusterService;
import org.everthrift.services.thrift.cluster.ClusterService.getNodeConfiguration_args;
import org.everthrift.services.thrift.cluster.Node;
import org.jetbrains.annotations.NotNull;

@RpcJGroups
public class GetNodeConfigurationController extends ThriftController<ClusterService.getNodeConfiguration_args, Node> {

    @Override
    public void setup(getNodeConfiguration_args args) {
        this.noProfile = true;
    }

    @NotNull
    @Override
    protected Node handle() throws TException {
        final RpcJGroupsRegistry r = context.getBean(RpcJGroupsRegistry.class);
        return r == null ? new Node() : r.getNodeConfiguration();
    }
}
