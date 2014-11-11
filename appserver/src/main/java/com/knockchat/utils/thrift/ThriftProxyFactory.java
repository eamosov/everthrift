package com.knockchat.utils.thrift;

import java.lang.reflect.Proxy;

import org.springframework.util.ClassUtils;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.SettableFuture;
import com.knockchat.utils.thrift.ThriftInvocationHandler.InvocationInfo;

public class ThriftProxyFactory {
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static <T> T service(Class<T> cls){
		
		final SettableFuture<InvocationInfo> f = SettableFuture.create();
		
		Futures.addCallback(f, new FutureCallback<InvocationInfo>(){

			@Override
			public void onSuccess(InvocationInfo result) {
				InvocationInfoThreadHolder.invocationInfo.set(result);				
			}

			@Override
			public void onFailure(Throwable t) {
				
			}});		
		
		return (T)ThriftProxyFactory.getProxy(cls, f);
	}
	
	/**
	 * Создать proxy для thrift интерейса. После вызова любого метода этого интерфейса произойдет вызов f.set()
	 * Вызвать можно не более одного метода для каждого интерфейса, т.к. f.set() можно сделать только 1 раз.
	 * 
	 * @param serviceIface
	 * @param f
	 * @return
	 */
	@SuppressWarnings("rawtypes")
	public static <T> T getProxy(Class serviceIface, SettableFuture<InvocationInfo> f){				
		return (T)Proxy.newProxyInstance(ClassUtils.getDefaultClassLoader(), new Class[]{serviceIface}, new ThriftInvocationHandler(serviceIface, f));
	}

	private ThriftProxyFactory() {
	}
	
}
