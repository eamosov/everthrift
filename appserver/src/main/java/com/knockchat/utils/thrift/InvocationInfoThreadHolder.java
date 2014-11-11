package com.knockchat.utils.thrift;

import com.knockchat.utils.thrift.ThriftInvocationHandler.InvocationInfo;

public class InvocationInfoThreadHolder {

	final static ThreadLocal<InvocationInfo<?>> invocationInfo = new ThreadLocal<InvocationInfo<?>>();
	
	public static InvocationInfo<?> getInvocationInfo(){
		return invocationInfo.get();
	}

}
