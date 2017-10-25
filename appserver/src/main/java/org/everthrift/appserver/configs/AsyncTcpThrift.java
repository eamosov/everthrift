package org.everthrift.appserver.configs;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.everthrift.appserver.transport.asynctcp.AsyncTcpThriftAdapter;
import org.everthrift.appserver.transport.asynctcp.RpcAsyncTcpRegistry;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.ImportResource;

import java.util.List;

@Configuration
@ImportResource("classpath:async-tcp-thrift.xml")
public class AsyncTcpThrift {

    @Bean
    public AsyncTcpThriftAdapter asyncTcpThriftAdapter() {
        return new AsyncTcpThriftAdapter(new TBinaryProtocol.Factory());
    }

    @Bean
    public RpcAsyncTcpRegistry rpcAsyncTcpRegistry() {
        return new RpcAsyncTcpRegistry();
    }
}
