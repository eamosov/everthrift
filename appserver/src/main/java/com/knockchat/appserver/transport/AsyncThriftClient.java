package com.knockchat.appserver.transport;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TMemoryBuffer;
import org.apache.thrift.transport.TTransport;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ListenableFuture;
import com.knockchat.utils.thrift.AbstractThriftClient;
import com.knockchat.utils.thrift.InvocationInfo;

public class AsyncThriftClient extends AbstractThriftClient<Void> {
	
	private final TTransport transport;
	private final AsyncRegister async;
	private final TProtocolFactory protocolFactory;

	public AsyncThriftClient(TTransport transport, AsyncRegister async, TProtocolFactory protocolFactory) {
		super(null);
		this.transport = transport;
		this.async = async;
		this.protocolFactory = protocolFactory;
	}

	@Override
	public void setSession(Object data) {		
	}

	@Override
	public Object getSession() {
		return null;
	}

	@Override
	public String getSessionId() {
		return null;
	}

	@Override
	public void addCloseCallback(FutureCallback<Void> callback) {
		// TODO Auto-generated method stub
		
	}

	@Override
	protected synchronized <T> ListenableFuture<T> thriftCall(Void sessionId, int timeout, InvocationInfo tInfo) throws TException {
		final int s = async.nextSeqId();
		final TMemoryBuffer payload = tInfo.buildCall(s, protocolFactory);
		async.put(s, tInfo);
		try{
			transport.write(payload.getArray(), 0, payload.length());
			transport.flush(s);
		}catch(TException e){
			async.pop(s);
			throw e;
		}
		return tInfo;
	}

}
