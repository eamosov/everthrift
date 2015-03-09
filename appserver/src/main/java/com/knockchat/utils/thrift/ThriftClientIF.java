package com.knockchat.utils.thrift;

import org.apache.thrift.TException;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ListenableFuture;

public interface ThriftClientIF {

	<T> ListenableFuture<T> thriftCall(int timeout, T methodCall) throws TException;	
	<T> ListenableFuture<T> thriftCall(int timeout, T methodCall, FutureCallback<T> callback) throws TException;		
	@SuppressWarnings({ "rawtypes"})
	<T> ListenableFuture<T> thriftCallByInfo(int timeout, InvocationInfo tInfo) throws TException;

	void setSession(Object data);
	Object getSession();
	
	String getSessionId();
	void addCloseCallback(FutureCallback<Void> callback);
}
