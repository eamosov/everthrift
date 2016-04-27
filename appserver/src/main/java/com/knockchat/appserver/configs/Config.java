package com.knockchat.appserver.configs;

import org.apache.thrift.protocol.TProtocolFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.ImportResource;
import org.springframework.context.annotation.Scope;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.knockchat.appserver.cluster.controllers.GetNodeConfigurationController;
import com.knockchat.appserver.controller.ThriftControllerJmx;
import com.knockchat.appserver.controller.ThriftControllerRegistry;
import com.knockchat.appserver.controller.ThriftProcessor;
import com.knockchat.appserver.model.LocalEventBus;
import com.knockchat.appserver.model.cassandra.CassandraFactories;

@Configuration
@ImportResource("classpath:app-context.xml")
public class Config {
        
    @Bean
    public ListeningExecutorService listeningCallerRunsBoundQueueExecutor(@Qualifier("callerRunsBoundQueueExecutor") ThreadPoolTaskExecutor executor){
    	return MoreExecutors.listeningDecorator(executor.getThreadPoolExecutor());
    }

    @Bean
    public ListeningScheduledExecutorService listeningScheduledExecutorService(@Qualifier("myScheduler") ThreadPoolTaskScheduler scheduler){
    	return MoreExecutors.listeningDecorator(scheduler.getScheduledThreadPoolExecutor());
    }
    
    @Bean
    @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    public ThriftProcessor thriftProcessor(ThriftControllerRegistry registry, TProtocolFactory protocolFactory) {
        return new ThriftProcessor(registry, protocolFactory);
    }
    
    @Bean
    public LocalEventBus localEventBus(){
    	return new LocalEventBus();
    }
    
    @Bean
    public ThriftControllerJmx ThriftControllerJmx(){
    	return new ThriftControllerJmx();
    }
    
    @Bean
    @Scope(value=ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    public GetNodeConfigurationController getNodeConfigurationController(){
    	return new GetNodeConfigurationController();
    }
    
    @Bean
    public CassandraFactories cassandraFactories(){
    	return new CassandraFactories();
    }
}
