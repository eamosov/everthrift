package org.everthrift.appserver.configs;

import com.google.common.collect.ImmutableList;
import org.everthrift.appserver.jgroups.LoopbackThriftClientServerImpl;
import org.everthrift.appserver.jgroups.RpcJGroupsRegistry;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;

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
