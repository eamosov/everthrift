package com.knockchat.appserver.configs;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.knockchat.appserver.jgroups.LoopbackThriftClientServerImpl;
import com.knockchat.appserver.jgroups.RpcJGroupsRegistry;

@Configuration
public class LoopbackJGroups {

    @Bean
    public RpcJGroupsRegistry RpcJGroupsRegistry(){
    	return new RpcJGroupsRegistry();
    }

    @Bean
    public LoopbackThriftClientServerImpl loopbackThriftClientServerImpl(){
    	return new LoopbackThriftClientServerImpl();
    }
}
