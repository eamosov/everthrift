package org.everthrift.appserver.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jmx.export.annotation.ManagedOperation;
import org.springframework.jmx.export.annotation.ManagedResource;

@ManagedResource(objectName = "bean:name=ThriftController")
public class ThriftControllerJmx {

    private static final Logger log = LoggerFactory.getLogger(ThriftControllerJmx.class);

    @ManagedOperation(description = "getExecutionLog")
    public String getExecutionLog() {
        return AbstractThriftController.getExecutionLog();
    }

    @ManagedOperation(description = "logExecutionLog")
    public void logExecutionLog() {
        log.info("\n{}", AbstractThriftController.getExecutionLog());
    }

    @ManagedOperation(description = "resetExecutionLog")
    public void resetExecutionLog() {
        AbstractThriftController.resetExecutionLog();
    }

}
