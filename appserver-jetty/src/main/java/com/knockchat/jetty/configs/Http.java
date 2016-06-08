package com.knockchat.jetty.configs;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TJSONProtocol;
import org.apache.thrift.transport.TKnockZlibTransport;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.ImportResource;
import org.springframework.messaging.SubscribableChannel;

import com.knockchat.jetty.JettyServer;
import com.knockchat.jetty.monitoring.RpsServlet;
import com.knockchat.jetty.transport.http.BinaryThriftServlet;
import com.knockchat.jetty.transport.http.JsonThriftServlet;
import com.knockchat.jetty.transport.http.RpcHttpRegistry;
import com.knockchat.jetty.transport.websocket.RpcWebsocketRegistry;
import com.knockchat.jetty.transport.websocket.WebsocketThriftHandler;

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
	
	@Bean
	public RpsServlet rpsServlet(){
		return new RpsServlet();
	}
	
	@Bean
	public WebsocketThriftHandler websocketThriftHandler(@Qualifier("inWebsocketChannel") SubscribableChannel inWebsocketChannel, @Qualifier("outWebsocketChannel") SubscribableChannel outWebsocketChannel){
		return new WebsocketThriftHandler(new TBinaryProtocol.Factory(), inWebsocketChannel, outWebsocketChannel);
	}
		
	@Bean
	public WebsocketThriftHandler ZlibWebsocketThriftHandler(@Qualifier("inZlibWebsocketChannel") SubscribableChannel inWebsocketChannel, @Qualifier("outZlibWebsocketChannel") SubscribableChannel outWebsocketChannel){
		final WebsocketThriftHandler h = new WebsocketThriftHandler(new TBinaryProtocol.Factory(), inWebsocketChannel, outWebsocketChannel);
		h.setTransportFactory(new TKnockZlibTransport.Factory());
		return h;
	}
	
	@Bean
	public WebsocketThriftHandler JSWebsocketThriftHandler(@Qualifier("inJSWebsocketChannel") SubscribableChannel inWebsocketChannel, @Qualifier("outJSWebsocketChannel") SubscribableChannel outWebsocketChannel){
		final WebsocketThriftHandler h = new WebsocketThriftHandler(new TJSONProtocol.Factory(), inWebsocketChannel, outWebsocketChannel);
		h.setContentType("TEXT");
		return h;
	}
	
	@Bean
	public JettyServer jettyServer(){
		return new JettyServer();
	}

}
