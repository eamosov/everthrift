package org.everthrift.rabbit;

import org.everthrift.appserver.controller.ThriftProcessor;
import org.everthrift.utils.ThriftServicesDb;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class LocalRabbitConfig {

    @Bean
    public RpcRabbitRegistry rpcRabbitRegistry() {
        return new RpcRabbitRegistry();
    }

    @Bean
    public ThriftProcessor rabbitThriftProcessor(RpcRabbitRegistry rpcRabbitRegistry) {
        return new ThriftProcessor(rpcRabbitRegistry);
    }

    @Bean
    public LocalRabbitThriftClientServerImpl localRabbitThriftClientServerImpl(@Qualifier("testMode") boolean testMode,
                                                                               @Qualifier("rabbitThriftProcessor") ThriftProcessor rabbitThriftProcessor,
                                                                               ThriftServicesDb thriftServicesDb) {

        final LocalRabbitThriftClientServerImpl impl = new LocalRabbitThriftClientServerImpl(testMode, rabbitThriftProcessor, thriftServicesDb);
        return impl;
    }

}
