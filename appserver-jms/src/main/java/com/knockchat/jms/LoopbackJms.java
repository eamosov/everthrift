package com.knockchat.jms;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

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
