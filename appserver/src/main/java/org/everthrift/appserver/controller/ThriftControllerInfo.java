package org.everthrift.appserver.controller;

import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.everthrift.appserver.utils.thrift.ThriftClient;
import org.everthrift.utils.ThriftServicesDb;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;

import java.lang.annotation.Annotation;

public class ThriftControllerInfo {

    private static final Logger log = LoggerFactory.getLogger(ThriftControllerInfo.class);

    private final String beanName;

    private final Class<? extends ThriftController> controllerCls;

    public final ThriftServicesDb.ThriftMethodEntry thriftMethodEntry;

    private final ApplicationContext context;

    public ThriftControllerInfo(ApplicationContext context, String beanName, Class<? extends ThriftController> controllerCls,
                                ThriftServicesDb.ThriftMethodEntry thriftMethodEntry) {
        super();
        this.beanName = beanName;
        this.controllerCls = controllerCls;
        this.thriftMethodEntry = thriftMethodEntry;
        this.context = context;
    }

    @NotNull
    public String getName() {
        return this.thriftMethodEntry.serviceName + ":" + this.thriftMethodEntry.methodName;
    }


    public ThriftController makeController(TBase args, ThriftProtocolSupportIF tps, LogEntry logEntry, int seqId, ThriftClient thriftClient,
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

    public Class<? extends ThriftController> getControllerCls() {
        return controllerCls;
    }

    public String getBeanName() {
        return beanName;
    }
}
