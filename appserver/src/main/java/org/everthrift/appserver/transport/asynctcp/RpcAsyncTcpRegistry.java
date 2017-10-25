package org.everthrift.appserver.transport.asynctcp;

import org.everthrift.appserver.controller.ThriftControllerRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.integration.ip.tcp.connection.AbstractServerConnectionFactory;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.List;

public class RpcAsyncTcpRegistry extends ThriftControllerRegistry {

    private static final Logger log = LoggerFactory.getLogger(RpcAsyncTcpRegistry.class);

    @Resource
    private AbstractServerConnectionFactory server;

    public RpcAsyncTcpRegistry() {
        super(RpcAsyncTcp.class);
    }

    @PostConstruct
    private void logHostPort() {
        log.info("Async thrift tpc server on {}:{}", server.getLocalAddress(), server.getPort());
    }

}
