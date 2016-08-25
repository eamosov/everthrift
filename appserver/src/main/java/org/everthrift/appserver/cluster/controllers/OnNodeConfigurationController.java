package org.everthrift.appserver.cluster.controllers;

import org.apache.thrift.TException;
import org.everthrift.appserver.controller.ThriftController;
import org.everthrift.appserver.jgroups.RpcJGroups;
import org.everthrift.clustering.jgroups.ClusterThriftClientImpl;
import org.everthrift.services.thrift.cluster.ClusterService;
import org.everthrift.services.thrift.cluster.ClusterService.onNodeConfiguration_args;
import org.jgroups.Address;
import org.springframework.beans.factory.annotation.Autowired;

@RpcJGroups
public class OnNodeConfigurationController extends ThriftController<ClusterService.onNodeConfiguration_args, Void> {

    @Autowired
    private ClusterThriftClientImpl client;

    @Override
    public void setup(onNodeConfiguration_args args) {
        this.noProfile = true;
    }

    @Override
    protected Void handle() throws TException {
        client.setNode((Address) tps.getAttributes().get("src"), args.getNode());
        return null;
    }

}
