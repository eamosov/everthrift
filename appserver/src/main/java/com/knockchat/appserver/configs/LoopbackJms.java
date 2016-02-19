package com.knockchat.appserver.configs;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.knockchat.appserver.cluster.LocalQueryThriftTransport;
import com.knockchat.appserver.transport.jms.RpcJmsRegistry;

@Configuration
public class LoopbackJms {
	
	@Bean
	public RpcJmsRegistry RpcJmsRegistry(){
		return new RpcJmsRegistry();
	}
	
    @Bean
    public LocalQueryThriftTransport localQueryThriftTransport(){
    	return new LocalQueryThriftTransport();
    }

}
