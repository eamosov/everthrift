package org.everthrift.jetty.configs;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TJSONProtocol;
import org.apache.thrift.transport.TKnockZlibTransport;
import org.everthrift.jetty.JettyServer;
import org.everthrift.jetty.monitoring.RpsServlet;
import org.everthrift.jetty.transport.http.BinaryThriftServlet;
import org.everthrift.jetty.transport.http.JsonThriftServlet;
import org.everthrift.jetty.transport.http.PlainJsonThriftServlet;
import org.everthrift.jetty.transport.http.RpcHttpRegistry;
import org.everthrift.jetty.transport.websocket.RpcWebsocketRegistry;
import org.everthrift.jetty.transport.websocket.WebsocketThriftHandler;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.ImportResource;
import org.springframework.messaging.SubscribableChannel;

import java.util.List;

@Configuration
@ImportResource("classpath:websocket-beans.xml")
public class Http {

    @Bean
    public RpcHttpRegistry rpcHttpRegistry(@Qualifier("thriftControllersPath") List<String> basePath) {
        return new RpcHttpRegistry(basePath);
    }

    @Bean
    public RpcWebsocketRegistry rpcWebsocketRegistry(@Qualifier("thriftControllersPath") List<String> basePath) {
        return new RpcWebsocketRegistry(basePath);
    }

    @Bean
    public JsonThriftServlet jsonThriftServlet() {
        return new JsonThriftServlet();
    }

    @Bean
    public BinaryThriftServlet binaryThriftServlet() {
        return new BinaryThriftServlet();
    }

    @Bean
    public PlainJsonThriftServlet plainJsonThriftServlet() {
        return new PlainJsonThriftServlet();
    }

    @Bean
    public RpsServlet rpsServlet() {
        return new RpsServlet();
    }

    @Bean
    public WebsocketThriftHandler websocketThriftHandler(@Qualifier("inWebsocketChannel") SubscribableChannel inWebsocketChannel,
                                                         @Qualifier("outWebsocketChannel") SubscribableChannel outWebsocketChannel) {
        return new WebsocketThriftHandler(new TBinaryProtocol.Factory(), inWebsocketChannel, outWebsocketChannel);
    }

    @Bean
    public WebsocketThriftHandler ZlibWebsocketThriftHandler(@Qualifier("inZlibWebsocketChannel") SubscribableChannel inWebsocketChannel,
                                                             @Qualifier("outZlibWebsocketChannel") SubscribableChannel outWebsocketChannel) {
        final WebsocketThriftHandler h = new WebsocketThriftHandler(new TBinaryProtocol.Factory(), inWebsocketChannel, outWebsocketChannel);
        h.setTransportFactory(new TKnockZlibTransport.Factory());
        return h;
    }

    @Bean
    public WebsocketThriftHandler JSWebsocketThriftHandler(@Qualifier("inJSWebsocketChannel") SubscribableChannel inWebsocketChannel,
                                                           @Qualifier("outJSWebsocketChannel") SubscribableChannel outWebsocketChannel) {
        final WebsocketThriftHandler h = new WebsocketThriftHandler(new TJSONProtocol.Factory(), inWebsocketChannel, outWebsocketChannel);
        h.setContentType("TEXT");
        return h;
    }

    @Bean
    public JettyServer jettyServer() {
        return new JettyServer();
    }

}
