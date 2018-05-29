package org.everthrift.appserver.configs;

import org.everthrift.appserver.controller.ThriftProcessor;
import org.everthrift.appserver.jgroups.LoopbackThriftClientServerImpl;
import org.everthrift.appserver.jgroups.RpcJGroups;
import org.everthrift.clustering.thrift.ThriftControllerDiscovery;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class LoopbackJGroups {

    @NotNull
    @Bean
    public ThriftProcessor jGroupsThriftProcessor() {
        return new ThriftProcessor(RpcJGroups.class);
    }

    @NotNull
    @Bean
    public LoopbackThriftClientServerImpl loopbackThriftClientServerImpl(ThriftControllerDiscovery thriftControllerDiscovery, @Qualifier("jGroupsThriftProcessor") ThriftProcessor thriftProcessor) {
        return new LoopbackThriftClientServerImpl(thriftControllerDiscovery, thriftProcessor);
    }
}
