package com.knockchat.clustering.jgroups;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import org.apache.thrift.TException;
import org.jgroups.Address;
import org.jgroups.blocks.ResponseMode;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.knockchat.clustering.thrift.InvocationInfo;
import com.knockchat.clustering.thrift.InvocationInfoThreadHolder;

public interface ClusterThriftClientIF {
	
	public static class Options{
		private Options(){			
		}
		
		public static Timeout timeout(int timeout){
			return new Timeout(timeout);
		}
		
		public static Loopback loopback(boolean loopback){
			return new Loopback(loopback);
		}
		
		public static Response responseMode(ResponseMode responseMode){
			return new Response(responseMode);
		}				
	}
	
	public static class Timeout extends Options{
		public final int timeout; 
		private Timeout(int timeout){
			this.timeout = timeout;
		}
	}
	
	public static class Loopback extends Options{
		public final boolean loopback;
		private Loopback(boolean loopback){
			this.loopback = loopback;
		}
	}
	
	public static class Response extends Options{
		public final ResponseMode responseMode;
		private Response(ResponseMode responseMode){
			this.responseMode = responseMode;
		}
	}
	
	default public <T> ListenableFuture<Map<Address, T>> call(T methodCall, Options ... options) throws TException{
		return call(InvocationInfoThreadHolder.getInvocationInfo(), options);
	}

	@SuppressWarnings("rawtypes")
	default public <T> ListenableFuture<Map<Address, T>> call(final InvocationInfo ii, Options ... options) throws TException {
		return call(null, null, ii, options);
	}		
	
	default public <T> ListenableFuture<T> call(Address destination, T methodCall, Options ... options) throws TException {
		return call(destination, InvocationInfoThreadHolder.getInvocationInfo(), options);
	}

	@SuppressWarnings("rawtypes")
	default public <T> ListenableFuture<T> call(Address destination, InvocationInfo ii, Options ... options) throws TException {
		final ListenableFuture<Map<Address, T>> ret = call(Collections.singleton(destination), null, ii, options);		
		return Futures.transform(ret, (Map<Address, T> m) -> (m.get(destination)));
	}		
	
	@SuppressWarnings("rawtypes")
	public <T> ListenableFuture<Map<Address, T>> call(Collection<Address> dest, Collection<Address> exclusionList, InvocationInfo tInfo, Options ... options) throws TException;
	
	default public <T> ListenableFuture<T> callOne(T methodCall, Options ... options) throws TException{
		return callOne(InvocationInfoThreadHolder.getInvocationInfo(), options);
	}
	
	@SuppressWarnings("rawtypes")
	public <T> ListenableFuture<T> callOne(InvocationInfo ii, Options ... options) throws TException;
}
