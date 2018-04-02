package org.everthrift.appserver.configs;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.everthrift.appserver.controller.ThriftProcessor;
import org.everthrift.appserver.transport.asynctcp.AsyncTcpThriftAdapter;
import org.everthrift.appserver.transport.asynctcp.RpcAsyncTcpRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.ImportResource;

@Configuration
@ImportResource("classpath:async-tcp-thrift.xml")
public class AsyncTcpThrift {

    private static final Logger log = LoggerFactory.getLogger(AsyncTcpThrift.class);

    @Bean
    public RpcAsyncTcpRegistry rpcAsyncTcpRegistry() {
        log.info("Starting bean: RpcAsyncTcpRegistry");
        return new RpcAsyncTcpRegistry();
    }

    @Bean
    public ThriftProcessor asyncTcpThriftProcessor(RpcAsyncTcpRegistry rpcAsyncTcpRegistry) {
        return new ThriftProcessor(rpcAsyncTcpRegistry);
    }

    @Bean
    public AsyncTcpThriftAdapter asyncTcpThriftAdapter(@Qualifier("asyncTcpThriftProcessor") ThriftProcessor asyncTcpThriftProcessor) {
        log.info("Starting bean: AsyncTcpThriftAdapter");
        return new AsyncTcpThriftAdapter(new TBinaryProtocol.Factory(), asyncTcpThriftProcessor);
    }
}
