package com.knockchat.appserver.configs;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.knockchat.appserver.transport.tcp.RpcSyncTcpRegistry;
import com.knockchat.appserver.transport.tcp.ThriftServer;

@Configuration
public class TcpThrift {
	
	@Bean
	public RpcSyncTcpRegistry rpcSyncTcpRegistry(){
		return new RpcSyncTcpRegistry();
	}

	@Bean
	public ThriftServer ThriftServer(ApplicationContext context, RpcSyncTcpRegistry registry){
		return new ThriftServer(context, new TBinaryProtocol.Factory(), registry);
	}
}
