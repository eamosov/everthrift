package com.knockchat.appserver.controller;

import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.TFieldIdEnum;
import org.apache.thrift.protocol.TProtocolFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.util.StringUtils;

import com.knockchat.utils.thrift.ThriftClient;

public class ThriftControllerInfo {
	private final Class<? extends ThriftController> controllerCls;
	private final String serviceName;
	private final String methodName;
	private Class<? extends TBase> argCls;
	private final Class<? extends TBase> resultCls;
	private final Method findResultFieldByName;
	private final ApplicationContext context;

	public ThriftControllerInfo(ApplicationContext context, Class<? extends ThriftController> controllerCls,
			String serviceName, String methodName,
			Class<? extends TBase> argCls, Class<? extends TBase> resultCls, Method findResultFieldByName) {
		super();
		this.controllerCls = controllerCls;
		this.serviceName = serviceName;
		this.methodName = methodName;
		this.argCls = argCls;
		this.resultCls = resultCls;
		this.findResultFieldByName = findResultFieldByName;
		this.context = context;		
	}

	public String getName(){
		return this.serviceName + ":" + this.methodName;
	}

	@Override
	public String toString() {
		return "ThriftControllerInfo [controllerCls=" + controllerCls
				+ ", serviceName=" + serviceName + ", methodName=" + methodName
				+ ", argCls=" + argCls + ", resultCls=" + resultCls + "]";
	}


	public TBase makeArgument(){
		try {
			return argCls.newInstance();
		} catch (InstantiationException e) {
			throw new RuntimeException(e);
		} catch (IllegalAccessException e) {
			throw new RuntimeException(e);
		}
	}
	
	public TBase makeResult(Object ret){
		
		try {

			final TBase res =  resultCls.newInstance();

			if (ret !=null){
				final TFieldIdEnum f;
				if (ret instanceof TException){				 
					f = (TFieldIdEnum)findResultFieldByName.invoke(null, StringUtils.uncapitalize(ret.getClass().getSimpleName()));
				}else{			
					f = (TFieldIdEnum)findResultFieldByName.invoke(null, "success");
				}
				
				if (f==null){
					throw new IllegalArgumentException("no such field for class " + ret.getClass().getSimpleName());
				}
				
				res.setFieldValue(f, ret);				
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
		}		
	}
	
	public ThriftController makeController(TBase args, MessageWrapper attributes, LogEntry logEntry, int seqId, ThriftClient thriftClient, Class<? extends Annotation> registryAnn, TProtocolFactory protocolFactory) throws TException{
		
		final ThriftController ctrl = context.getBean(controllerCls);
		ctrl.setup(args, this, attributes, logEntry, seqId, thriftClient, registryAnn, protocolFactory);
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
}
