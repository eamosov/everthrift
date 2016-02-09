package com.knockchat.node.model;

import javax.annotation.Resource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Component;

import com.google.common.eventbus.AsyncEventBus;
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.SubscriberExceptionContext;
import com.google.common.eventbus.SubscriberExceptionHandler;

@Component
public class LocalEventBus implements InitializingBean{
	
	private static final Logger log = LoggerFactory.getLogger(LocalEventBus.class);

	@Resource
	private ThreadPoolTaskExecutor myExecutor;

    private EventBus eventBus;
	private EventBus asyncEventBus;
	
	public LocalEventBus() {

	}

	@Override
	public void afterPropertiesSet() throws Exception {		
		eventBus = new EventBus(new SubscriberExceptionHandler(){

			@Override
			public void handleException(Throwable exception, SubscriberExceptionContext context) {
				log.error("Exception in eventBus", exception);
			}});
		
		asyncEventBus = new AsyncEventBus(myExecutor, new SubscriberExceptionHandler(){

			@Override
			public void handleException(Throwable exception, SubscriberExceptionContext context) {
				log.error("Exception in asyncEventBus", exception);
			}});
	}

	public void register(Object object){
		eventBus.register(object);
		asyncEventBus.register(object);
	}
	
	public void post(Object event){
		eventBus.post(event);
	}
	
	public void postAsync(Object event){
		asyncEventBus.post(event);
	}
	
}
