package com.knockchat.appserver.configs;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.knockchat.appserver.cluster.LoopbackClusterThriftTransport;
import com.knockchat.appserver.transport.jgroups.RpcJGroupsRegistry;

@Configuration
public class LoopbackJGroups {

    @Bean
    public RpcJGroupsRegistry RpcJGroupsRegistry(){
    	return new RpcJGroupsRegistry();
    }

    @Bean
    public LoopbackClusterThriftTransport loopbackClusterThriftTransport(){
    	return new LoopbackClusterThriftTransport();
    }
}
