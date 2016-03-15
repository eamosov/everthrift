package com.knockchat.appserver.configs;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.ImportResource;

import com.knockchat.appserver.transport.http.BinaryThriftServlet;
import com.knockchat.appserver.transport.http.JsonThriftServlet;
import com.knockchat.appserver.transport.http.RpcHttpRegistry;
import com.knockchat.appserver.transport.websocket.RpcWebsocketRegistry;

@Configuration
@ImportResource("classpath:websocket-beans.xml")
public class Http {

	@Bean
	public RpcHttpRegistry rpcHttpRegistry(){
		return new RpcHttpRegistry();
	}
	
	@Bean
	public RpcWebsocketRegistry rpcWebsocketRegistry(){
		return new RpcWebsocketRegistry();
	}
	
	@Bean
	public JsonThriftServlet jsonThriftServlet(){
		return new JsonThriftServlet();
	}
	
	@Bean
	public BinaryThriftServlet binaryThriftServlet(){
		return new BinaryThriftServlet();
	}
}
