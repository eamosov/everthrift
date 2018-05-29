package org.everthrift.jetty.configs;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TJSONProtocol;
import org.apache.thrift.transport.TKnockZlibTransport;
import org.everthrift.appserver.controller.ThriftProcessor;
import org.everthrift.appserver.transport.http.RpcHttp;
import org.everthrift.appserver.transport.websocket.RpcWebsocket;
import org.everthrift.jetty.JettyServer;
import org.everthrift.jetty.monitoring.RpsServlet;
import org.everthrift.jetty.transport.http.BinaryThriftServlet;
import org.everthrift.jetty.transport.http.JsonThriftServlet;
import org.everthrift.jetty.transport.http.PlainJsonThriftServlet;
import org.everthrift.jetty.transport.websocket.WebsocketThriftHandler;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.ImportResource;
import org.springframework.messaging.SubscribableChannel;

@Configuration
@ImportResource("classpath:websocket-beans.xml")
public class Http {


    @Bean
    public ThriftProcessor httpProcessor() {
        return new ThriftProcessor(RpcHttp.class);
    }

    @Bean
    public ThriftProcessor websocketProcessor() {
        return new ThriftProcessor(RpcWebsocket.class);
    }

    @Bean
    public JsonThriftServlet jsonThriftServlet(@Qualifier("httpProcessor") ThriftProcessor httpProcessor) {
        return new JsonThriftServlet(httpProcessor);
    }

    @Bean
    public BinaryThriftServlet binaryThriftServlet(@Qualifier("httpProcessor") ThriftProcessor httpProcessor) {
        return new BinaryThriftServlet(httpProcessor);
    }

    @Bean
    public PlainJsonThriftServlet plainJsonThriftServlet(@Qualifier("httpProcessor") ThriftProcessor httpProcessor) {
        return new PlainJsonThriftServlet(httpProcessor);
    }

    @Bean
    public RpsServlet rpsServlet() {
        return new RpsServlet();
    }

    @Bean
    public WebsocketThriftHandler websocketThriftHandler(@Qualifier("inWebsocketChannel") SubscribableChannel inWebsocketChannel,
                                                         @Qualifier("outWebsocketChannel") SubscribableChannel outWebsocketChannel,
                                                         @Qualifier("websocketProcessor") ThriftProcessor websocketProcessor
    ) {
        return new WebsocketThriftHandler(new TBinaryProtocol.Factory(), inWebsocketChannel, outWebsocketChannel, websocketProcessor);
    }

    @Bean
    public WebsocketThriftHandler ZlibWebsocketThriftHandler(@Qualifier("inZlibWebsocketChannel") SubscribableChannel inWebsocketChannel,
                                                             @Qualifier("outZlibWebsocketChannel") SubscribableChannel outWebsocketChannel,
                                                             @Qualifier("websocketProcessor") ThriftProcessor websocketProcessor) {
        final WebsocketThriftHandler h = new WebsocketThriftHandler(new TBinaryProtocol.Factory(), inWebsocketChannel, outWebsocketChannel, websocketProcessor);
        h.setTransportFactory(new TKnockZlibTransport.Factory());
        return h;
    }

    @Bean
    public WebsocketThriftHandler JSWebsocketThriftHandler(@Qualifier("inJSWebsocketChannel") SubscribableChannel inWebsocketChannel,
                                                           @Qualifier("outJSWebsocketChannel") SubscribableChannel outWebsocketChannel,
                                                           @Qualifier("websocketProcessor") ThriftProcessor websocketProcessor) {
        final WebsocketThriftHandler h = new WebsocketThriftHandler(new TJSONProtocol.Factory(), inWebsocketChannel, outWebsocketChannel, websocketProcessor);
        h.setContentType("TEXT");
        return h;
    }

    @Bean
    public JettyServer jettyServer() {
        return new JettyServer();
    }

}
