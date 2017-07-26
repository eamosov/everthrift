package org.everthrift.appserver.configs;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.everthrift.appserver.transport.tcp.RpcSyncTcpRegistry;
import org.everthrift.appserver.transport.tcp.ThriftServer;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;

@Configuration
public class TcpThrift {

    @Bean
    public RpcSyncTcpRegistry rpcSyncTcpRegistry(@Qualifier("thriftControllersPath") List<String> basePath) {
        return new RpcSyncTcpRegistry(basePath);
    }

    @Bean
    public ThriftServer ThriftServer(ApplicationContext context, RpcSyncTcpRegistry registry) {
        return new ThriftServer(context, new TBinaryProtocol.Factory(), registry);
    }
}
