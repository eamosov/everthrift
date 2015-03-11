package com.knockchat.utils.thrift;

import java.lang.reflect.Proxy;

public class ThriftProxyFactory {
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static <T> T onIfaceAsAsync(Class<T> cls){
				
		return (T)Proxy.newProxyInstance(ThriftProxyFactory.class.getClassLoader(), new Class[]{cls}, new ServiceIfaceProxy(cls, new InvocationCallback(){

			@Override
			public Object call(InvocationInfo ii) throws NullResult {
				InvocationInfoThreadHolder.invocationInfo.set(ii);
				throw new NullResult();
			}}));
	}
	
}
