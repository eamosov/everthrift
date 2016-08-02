package org.everthrift.rabbit;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class LocalRabbitConfig {

    @Bean
    public RpcRabbitRegistry rpcRabbitRegistry(){
        return new RpcRabbitRegistry();
    }

    @Bean
    public LocalRabbitThriftClientServerImpl localRabbitThriftClientServerImpl(@Qualifier("testMode") boolean testMode){
        final LocalRabbitThriftClientServerImpl impl =  new LocalRabbitThriftClientServerImpl();
        
        if (testMode)
            impl.setBlock(true);
        
        return impl;
    }

}
