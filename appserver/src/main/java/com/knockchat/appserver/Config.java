package com.knockchat.appserver;

import org.apache.thrift.protocol.TProtocolFactory;
import org.jgroups.JChannel;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.ImportResource;
import org.springframework.context.annotation.Scope;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.knockchat.appserver.controller.ThriftControllerRegistry;
import com.knockchat.appserver.controller.ThriftProcessor;

@Configuration
@ImportResource("classpath:app-context.xml")
@EnableJpaRepositories(basePackages={"com.knockchat"})
@ComponentScan("com.knockchat")
public class Config {
        
    @Bean
    public JChannel yocluster() throws Exception{
    	return new JChannel("jgroups.xml");
    }
        
    @Bean
    public ListeningExecutorService listeningExecutorService(@Qualifier("myExecutor") ThreadPoolTaskExecutor myExecutor){
    	return MoreExecutors.listeningDecorator(myExecutor.getThreadPoolExecutor());
    }

    @Bean
    public ListeningScheduledExecutorService listeningScheduledExecutorService(@Qualifier("myScheduler") ThreadPoolTaskScheduler myScheduler){
    	return MoreExecutors.listeningDecorator(myScheduler.getScheduledThreadPoolExecutor());
    }
    
    @Bean
    @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    public ThriftProcessor thriftProcessor(ThriftControllerRegistry registry, TProtocolFactory protocolFactory) {
        return new ThriftProcessor(registry, protocolFactory);
    }

}
