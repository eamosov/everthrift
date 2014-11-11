package com.knockchat.appserver.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jmx.export.annotation.ManagedOperation;
import org.springframework.jmx.export.annotation.ManagedResource;
import org.springframework.stereotype.Component;

@ManagedResource(objectName="bean:name=ThriftController")
@Component
public class ThriftControllerJmx {
	
	private static final Logger log = LoggerFactory.getLogger(ThriftControllerJmx.class); 

	@ManagedOperation(description="getExecutionLog")
	public String getExecutionLog(){
		return ThriftController.getExecutionLog();
	}

	@ManagedOperation(description="logExecutionLog")
	public void logExecutionLog(){
		log.info("\n{}", ThriftController.getExecutionLog());
	}

	@ManagedOperation(description="resetExecutionLog")
	public void resetExecutionLog(){
		ThriftController.resetExecutionLog();
	}

}
