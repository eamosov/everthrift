package org.everthrift.appserver.controller;

import org.apache.thrift.TApplicationException;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.TFieldIdEnum;
import org.everthrift.appserver.utils.thrift.ThriftClient;
import org.everthrift.utils.ClassUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;

import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.NoSuchElementException;

public class ThriftControllerInfo {

    private static final Logger log = LoggerFactory.getLogger(ThriftControllerInfo.class);

    private final Class<? extends ThriftController> controllerCls;

    private final String serviceName;

    private final String methodName;

    private Class<? extends TBase> argCls;

    private final Class<? extends TBase> resultCls;

    private final Method findResultFieldByName;

    private final ApplicationContext context;

    public ThriftControllerInfo(ApplicationContext context, Class<? extends ThriftController> controllerCls, String serviceName,
                                String methodName, Class<? extends TBase> argCls, Class<? extends TBase> resultCls,
                                Method findResultFieldByName) {
        super();
        this.controllerCls = controllerCls;
        this.serviceName = serviceName;
        this.methodName = methodName;
        this.argCls = argCls;
        this.resultCls = resultCls;
        this.findResultFieldByName = findResultFieldByName;
        this.context = context;
    }

    public String getName() {
        return this.serviceName + ":" + this.methodName;
    }

    @Override
    public String toString() {
        return "ThriftControllerInfo [controllerCls=" + controllerCls + ", serviceName=" + serviceName + ", methodName=" + methodName
            + ", argCls=" + argCls + ", resultCls=" + resultCls + "]";
    }

    public TBase makeArgument() {
        try {
            return argCls.newInstance();
        } catch (InstantiationException e) {
            throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    public TBase makeResult(Object ret) throws TApplicationException{

        try {

            final TBase res = resultCls.newInstance();

            if (ret != null) {
                if (ret instanceof TException) {
                    try{
                        final Method setMethod = ClassUtils.getPropertyDescriptors(resultCls).entrySet().stream()
                                                           .filter(e -> e.getValue().getReadMethod() != null
                                                               && e.getValue().getWriteMethod() != null && e.getValue()
                                                                                                            .getReadMethod()
                                                                                                            .getReturnType()
                                                                                                            .isAssignableFrom(ret.getClass()))
                                                           .findFirst().get().getValue().getWriteMethod();
                        setMethod.invoke(res, ret);
                    }catch (NoSuchElementException e){
                        log.error("Coudn't find Exception of type {} in {}", ret.getClass().getCanonicalName(), resultCls.getCanonicalName());
                        throw new TApplicationException(TApplicationException.INTERNAL_ERROR, ret.getClass().getSimpleName() + ":" + ((TException) ret).getMessage());
                    }
                } else {
                    final TFieldIdEnum f = (TFieldIdEnum) findResultFieldByName.invoke(null, "success");
                    if (f == null) {
                        throw new IllegalArgumentException("no such field for class " + ret.getClass().getSimpleName(),
                                                           (ret instanceof Throwable) ? (Throwable) ret : null);
                    }
                    res.setFieldValue(f, ret);
                }
            }
            return res;
        } catch (InstantiationException e) {
            throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        } catch (IllegalArgumentException e) {
            throw new RuntimeException(e);
        } catch (InvocationTargetException e) {
            throw new RuntimeException(e);
        } catch (NoSuchElementException e) {
            throw new RuntimeException(e);
        }
    }

    public ThriftController makeController(TBase args, ThriftProtocolSupportIF tps, LogEntry logEntry, int seqId, ThriftClient thriftClient,
                                           Class<? extends Annotation> registryAnn, boolean allowAsyncAnswer) throws TException {

        final ThriftController ctrl = context.getBean(controllerCls);
        ctrl.setup(args, this, tps, logEntry, seqId, thriftClient, registryAnn, allowAsyncAnswer);
        return ctrl;
    }

    public String getServiceName() {
        return serviceName;
    }

    public String getMethodName() {
        return methodName;
    }

    public Class<? extends ThriftController> getControllerCls() {
        return controllerCls;
    }

    public Class<? extends TBase> getResultCls() {
        return resultCls;
    }

    public Class<? extends TBase> getArgCls() {
        return argCls;
    }
}
