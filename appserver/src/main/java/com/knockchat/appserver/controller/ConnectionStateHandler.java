package com.knockchat.appserver.controller;

import java.lang.annotation.Annotation;

import javax.sql.DataSource;

import org.apache.thrift.protocol.TProtocolFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.transaction.TransactionStatus;

import com.knockchat.utils.thrift.ThriftClient;

public abstract class ConnectionStateHandler {
	
	protected final Logger log = LoggerFactory.getLogger(this.getClass());
	
	protected ThriftControllerInfo info;
	protected DataSource ds;
	protected TransactionStatus transactionStatus;
	protected ThriftClient thriftClient;
	protected MessageWrapper attributes;
	
	@Autowired
	private ApplicationContext context;
		
	protected Class<? extends Annotation> registryAnn;
	protected TProtocolFactory protocolFactory;

	public abstract boolean onOpen();
	
	public void setup (MessageWrapper attributes, ThriftClient thriftClient, Class<? extends Annotation> registryAnn, TProtocolFactory protocolFactory){
		this.thriftClient = thriftClient;
		this.registryAnn = registryAnn;
		this.protocolFactory = protocolFactory;
		this.attributes = attributes;		
		
		try{
			this.ds = context.getBean(DataSource.class);
		}catch (NoSuchBeanDefinitionException e){
			this.ds = null;
		}				
	}

}
