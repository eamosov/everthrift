package com.knockchat.utils.thrift;

import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.knockchat.utils.thrift.ThriftInvocationHandler.InvocationInfo;

public abstract class AbstractThriftClient<S> implements ThriftClient {
	
	private final S sessionId;

	public AbstractThriftClient(S sessionId) {
		this.sessionId = sessionId;
	}

	@Override
	public <T> ListenableFuture<T> thriftCall(int timeout, T methodCall, FutureCallback<T> callback) throws TException{
		final ListenableFuture<T> lf = thriftCall(timeout, methodCall);
		Futures.addCallback(lf,callback);
		return lf;
	}	

	@SuppressWarnings("rawtypes")
	@Override
	public <T> ListenableFuture<T> thriftCall(int timeout, T methodCall) throws TException{
		final InvocationInfo info = InvocationInfoThreadHolder.getInvocationInfo();
		if (info == null)
			throw new TTransportException("info is NULL");
		
		return thriftCall(sessionId, timeout, info);
	}
	
	@SuppressWarnings("rawtypes")
	@Override
	public <T> ListenableFuture<T> thriftCallByInfo(int timeout, InvocationInfo tInfo) throws TException {
		return thriftCall(sessionId, timeout, tInfo);
	}
			
	@SuppressWarnings("rawtypes")
	protected abstract <T> ListenableFuture<T> thriftCall(S sessionId, int timeout, InvocationInfo tInfo) throws TException;
}
