package org.everthrift.appserver.configs;

import org.everthrift.appserver.cluster.controllers.GetNodeConfigurationController;
import org.everthrift.appserver.cluster.controllers.OnNodeConfigurationController;
import org.everthrift.appserver.controller.ThriftControllerJmx;
import org.everthrift.appserver.controller.ThriftControllerRegistry;
import org.everthrift.appserver.controller.ThriftProcessor;
import org.everthrift.appserver.model.LocalEventBus;
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

@Configuration
@ImportResource("classpath:app-context.xml")
public class AppserverConfig {
	
	public AppserverConfig(){
		
	}
        
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
    public ThriftProcessor thriftProcessor(ThriftControllerRegistry registry) {
        return new ThriftProcessor(registry);
    }
    
    @Bean
    public LocalEventBus LocalEventBus(){
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
    @Scope(value=ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    public OnNodeConfigurationController getOnNodeConfigurationController(){
    	return new OnNodeConfigurationController();
    }    
    
}
