package org.everthrift.jms;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;

@Configuration
public class LoopbackJms {

    @Bean
    public LocalJmsThriftClientServerImpl localJmsThriftClientServerImpl(@Qualifier("testMode") boolean testMode) {
        return new LocalJmsThriftClientServerImpl(testMode);
    }

}
