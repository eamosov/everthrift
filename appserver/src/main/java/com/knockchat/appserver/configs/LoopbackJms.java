package com.knockchat.appserver.configs;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.knockchat.appserver.jms.LocalJmsThriftClientServerImpl;
import com.knockchat.appserver.jms.RpcJmsRegistry;

@Configuration
public class LoopbackJms {
	
	@Bean
	public RpcJmsRegistry RpcJmsRegistry(){
		return new RpcJmsRegistry();
	}
	
    @Bean
    public LocalJmsThriftClientServerImpl localJmsThriftClientServerImpl(){
    	return new LocalJmsThriftClientServerImpl();
    }

}
