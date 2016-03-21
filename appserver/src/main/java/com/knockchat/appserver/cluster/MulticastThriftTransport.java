package com.knockchat.appserver.cluster;

import java.util.Map;

import org.apache.thrift.TException;
import org.jgroups.Address;
import org.jgroups.blocks.ResponseMode;

import com.google.common.util.concurrent.ListenableFuture;
import com.knockchat.utils.thrift.InvocationInfo;

public interface MulticastThriftTransport {
	public <T> ListenableFuture<Map<Address, T>> thriftCall(boolean loopBack, int timeout, int seqId, ResponseMode responseMode,  final InvocationInfo ii) throws TException;
	public <T> ListenableFuture<Map<Address, T>> thriftCall(boolean loopBack, int timeout, int seqId, ResponseMode responseMode,  T methodCall) throws TException;
	
	//public <T> Map<Address, T> thriftCall(int timeout, int seqId, ResponseMode responseMode, InvocationInfo tInfo) throws TException;
}
