package com.knockchat.appserver.controller;

import org.apache.thrift.protocol.TProtocolFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

@Component
public class ThriftProcessorFactory {
	
	@Autowired
	private ApplicationContext context;

	public ThriftProcessor getThriftProcessor(ThriftControllerRegistry registry, TProtocolFactory protocolFactory){
		return (ThriftProcessor)context.getBean("thriftProcessor", registry, protocolFactory);
	}
}
