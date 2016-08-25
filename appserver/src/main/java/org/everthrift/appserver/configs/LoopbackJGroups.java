package org.everthrift.appserver.configs;

import org.everthrift.appserver.jgroups.LoopbackThriftClientServerImpl;
import org.everthrift.appserver.jgroups.RpcJGroupsRegistry;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class LoopbackJGroups {

    @Bean
    public RpcJGroupsRegistry RpcJGroupsRegistry() {
        return new RpcJGroupsRegistry();
    }

    @Bean
    public LoopbackThriftClientServerImpl loopbackThriftClientServerImpl() {
        return new LoopbackThriftClientServerImpl();
    }
}
