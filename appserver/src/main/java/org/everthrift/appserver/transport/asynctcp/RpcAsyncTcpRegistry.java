package org.everthrift.appserver.transport.asynctcp;

import org.everthrift.appserver.controller.ThriftControllerRegistry;
import org.springframework.integration.ip.tcp.connection.AbstractServerConnectionFactory;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;

public class RpcAsyncTcpRegistry extends ThriftControllerRegistry {

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
