package org.everthrift.rabbit;

import org.everthrift.appserver.controller.ThriftProcessor;
import org.everthrift.appserver.transport.rabbit.RpcRabbit;
import org.everthrift.thrift.ThriftServicesDiscovery;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class LocalRabbitConfig {

    @Bean
    public ThriftProcessor rabbitThriftProcessor() {
        return new ThriftProcessor(RpcRabbit.class);
    }

    @Bean
    public LocalRabbitThriftClientServerImpl localRabbitThriftClientServerImpl(@Qualifier("testMode") boolean testMode,
                                                                               @Qualifier("rabbitThriftProcessor") ThriftProcessor rabbitThriftProcessor,
                                                                               ThriftServicesDiscovery thriftServicesDb) {

        return new LocalRabbitThriftClientServerImpl(testMode, rabbitThriftProcessor, thriftServicesDb);
    }

}
