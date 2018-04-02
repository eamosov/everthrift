package org.everthrift.appserver.configs;

import org.everthrift.appserver.controller.ThriftProcessor;
import org.everthrift.appserver.jgroups.LoopbackThriftClientServerImpl;
import org.everthrift.appserver.jgroups.RpcJGroupsRegistry;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class LoopbackJGroups {

    @NotNull
    @Bean
    public RpcJGroupsRegistry RpcJGroupsRegistry() {
        return new RpcJGroupsRegistry();
    }

    @NotNull
    @Bean
    public ThriftProcessor jGroupsThriftProcessor(RpcJGroupsRegistry registry) {
        return new ThriftProcessor(registry);
    }

    @NotNull
    @Bean
    public LoopbackThriftClientServerImpl loopbackThriftClientServerImpl(@Qualifier("jGroupsThriftProcessor") ThriftProcessor thriftProcessor) {
        return new LoopbackThriftClientServerImpl(thriftProcessor);
    }
}
