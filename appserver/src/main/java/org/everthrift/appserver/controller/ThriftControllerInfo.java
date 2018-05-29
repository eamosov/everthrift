package org.everthrift.appserver.controller;

import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.everthrift.appserver.utils.thrift.ThriftClient;
import org.everthrift.thrift.ThriftServicesDiscovery;
import org.jetbrains.annotations.NotNull;
import org.springframework.context.ApplicationContext;

import java.lang.annotation.Annotation;

public class ThriftControllerInfo {

    private final String beanName;
    public final ThriftServicesDiscovery.ThriftMethodEntry thriftMethodEntry;
    public final Class beanClass;

    public ThriftControllerInfo(String beanName, Class beanClass, ThriftServicesDiscovery.ThriftMethodEntry thriftMethodEntry) {
        super();
        this.beanName = beanName;
        this.beanClass = beanClass;
        this.thriftMethodEntry = thriftMethodEntry;
    }

    @NotNull
    public String getName() {
        return this.thriftMethodEntry.serviceName + ":" + this.thriftMethodEntry.methodName;
    }


    public ThriftController makeController(ApplicationContext context, TBase args, ThriftProtocolSupportIF tps, LogEntry logEntry, int seqId, ThriftClient thriftClient,
                                           Class<? extends Annotation> registryAnn, boolean allowAsyncAnswer) throws TException {

        final ThriftController ctrl = context.getBean(beanName, ThriftController.class);
        ctrl.setup(args, this, tps, logEntry, seqId, thriftClient, registryAnn, allowAsyncAnswer, thriftMethodEntry.serviceName, thriftMethodEntry.methodName);

        return ctrl;
    }

    public String getServiceName() {
        return thriftMethodEntry.serviceName;
    }

    public String getMethodName() {
        return thriftMethodEntry.methodName;
    }

    public String getBeanName() {
        return beanName;
    }
}
