package com.knockchat.appserver.configs;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.ImportResource;

import com.knockchat.appserver.transport.asynctcp.AsyncTcpThriftAdapter;
import com.knockchat.appserver.transport.asynctcp.RpcAsyncTcpRegistry;

@Configuration
@ImportResource("classpath:async-tcp-thrift.xml")
public class AsyncTcpThrift {

	@Bean
	public AsyncTcpThriftAdapter asyncTcpThriftAdapter(){
		return new AsyncTcpThriftAdapter(new TBinaryProtocol.Factory());
	}
	
	@Bean
	public RpcAsyncTcpRegistry rpcAsyncTcpRegistry(){
		return new RpcAsyncTcpRegistry();
	}
}
