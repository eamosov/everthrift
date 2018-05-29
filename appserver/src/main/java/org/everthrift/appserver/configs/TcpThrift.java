package org.everthrift.appserver.configs;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.everthrift.appserver.controller.ThriftProcessor;
import org.everthrift.appserver.transport.tcp.RpcSyncTcp;
import org.everthrift.appserver.transport.tcp.ThriftServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class TcpThrift {

    private static final Logger log = LoggerFactory.getLogger(TcpThrift.class);

    @Bean
    public ThriftProcessor tcpThriftProcessor() {
        return new ThriftProcessor(RpcSyncTcp.class);
    }

    @Bean
    public ThriftServer ThriftServer(@Qualifier("tcpThriftProcessor") ThriftProcessor thriftProcessor) {
        log.info("Starting bean: ThriftServer");
        return new ThriftServer(new TBinaryProtocol.Factory(), thriftProcessor);
    }
}
