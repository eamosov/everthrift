package com.knockchat.appserver.configs;

import org.apache.thrift.protocol.TProtocolFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.FilterType;
import org.springframework.context.annotation.ImportResource;
import org.springframework.context.annotation.Scope;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.knockchat.appserver.controller.ThriftControllerRegistry;
import com.knockchat.appserver.controller.ThriftProcessor;

@Configuration
@ImportResource("classpath:app-context.xml")
@ComponentScan(value="com.knockchat", excludeFilters=@ComponentScan.Filter(type=FilterType.ASPECTJ, pattern="com.knockchat.appserver.configs.*"))
public class Config {
        
    @Bean
    public ListeningExecutorService listeningSyncQueueExecutor(@Qualifier("syncQueueExecutor") ThreadPoolTaskExecutor executor){
    	return MoreExecutors.listeningDecorator(executor.getThreadPoolExecutor());
    }

    @Bean
    public ListeningExecutorService listeningCallerRunsBoundQueueExecutor(@Qualifier("callerRunsBoundQueueExecutor") ThreadPoolTaskExecutor executor){
    	return MoreExecutors.listeningDecorator(executor.getThreadPoolExecutor());
    }

    @Bean
    public ListeningExecutorService listeningAbortBoundQueueExecutor(@Qualifier("abortBoundQueueExecutor") ThreadPoolTaskExecutor executor){
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
}
