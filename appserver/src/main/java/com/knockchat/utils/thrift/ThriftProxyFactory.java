package com.knockchat.utils.thrift;

import java.lang.reflect.Proxy;

import com.knockchat.utils.thrift.ThriftInvocationHandler.InvocationCallback;
import com.knockchat.utils.thrift.ThriftInvocationHandler.InvocationInfo;
import com.knockchat.utils.thrift.ThriftInvocationHandler.NullResult;

public class ThriftProxyFactory {
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static <T> T asyncService(Class<T> cls){
				
		return (T)Proxy.newProxyInstance(ThriftProxyFactory.class.getClassLoader(), new Class[]{cls}, new ThriftInvocationHandler(cls, new InvocationCallback(){

			@Override
			public Object call(InvocationInfo ii) throws NullResult {
				InvocationInfoThreadHolder.invocationInfo.set(ii);
				throw new NullResult();
			}}));
	}
	
}
