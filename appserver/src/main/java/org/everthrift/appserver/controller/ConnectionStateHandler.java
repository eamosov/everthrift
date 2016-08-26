package org.everthrift.appserver.controller;

import org.everthrift.appserver.utils.thrift.ThriftClient;
import org.everthrift.clustering.MessageWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;

import javax.sql.DataSource;
import java.lang.annotation.Annotation;

public abstract class ConnectionStateHandler {

    protected final Logger log = LoggerFactory.getLogger(this.getClass());

    protected ThriftControllerInfo info;

    protected DataSource ds;

    protected ThriftClient thriftClient;

    protected MessageWrapper attributes;

    @Autowired
    private ApplicationContext context;

    protected Class<? extends Annotation> registryAnn;

    public abstract boolean onOpen();

    public void setup(MessageWrapper attributes, ThriftClient thriftClient, Class<? extends Annotation> registryAnn) {
        this.thriftClient = thriftClient;
        this.registryAnn = registryAnn;
        this.attributes = attributes;

        try {
            this.ds = context.getBean(DataSource.class);
        } catch (NoSuchBeanDefinitionException e) {
            this.ds = null;
        }
    }

}
