package org.everthrift.appserver.configs;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.everthrift.appserver.transport.tcp.RpcSyncTcpRegistry;
import org.everthrift.appserver.transport.tcp.ThriftServer;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class TcpThrift {

    private static final Logger log = LoggerFactory.getLogger(TcpThrift.class);

    @NotNull
    @Bean
    public RpcSyncTcpRegistry rpcSyncTcpRegistry() {
        log.info("Starting bean: RpcSyncTcpRegistry");
        return new RpcSyncTcpRegistry();
    }

    @NotNull
    @Bean
    public ThriftServer ThriftServer(ApplicationContext context, RpcSyncTcpRegistry registry) {
        log.info("Starting bean: ThriftServer");
        return new ThriftServer(context, new TBinaryProtocol.Factory(), registry);
    }
}
