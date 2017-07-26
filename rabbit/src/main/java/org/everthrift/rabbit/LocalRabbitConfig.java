package org.everthrift.rabbit;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;

@Configuration
public class LocalRabbitConfig {

    @Bean
    public RpcRabbitRegistry rpcRabbitRegistry(@Qualifier("thriftControllersPath") List<String> basePath) {
        return new RpcRabbitRegistry(basePath);
    }

    @Bean
    public LocalRabbitThriftClientServerImpl localRabbitThriftClientServerImpl(@Qualifier("testMode") boolean testMode) {
        final LocalRabbitThriftClientServerImpl impl = new LocalRabbitThriftClientServerImpl();

        if (testMode) {
            impl.setBlock(true);
        }

        return impl;
    }

}
