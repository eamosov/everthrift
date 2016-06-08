package com.knockchat.clustering.jgroups;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.thrift.TApplicationException;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.jgroups.Address;
import org.jgroups.Message;
import org.jgroups.Message.TransientFlag;
import org.jgroups.blocks.MessageDispatcher;
import org.jgroups.blocks.RequestOptions;
import org.jgroups.blocks.ResponseMode;
import org.jgroups.util.FutureListener;
import org.jgroups.util.Rsp;
import org.jgroups.util.RspList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.knockchat.clustering.MessageWrapper;
import com.knockchat.clustering.thrift.InvocationInfo;

public abstract class AbstractJgroupsThriftClientImpl extends ClusterThriftClientImpl{
	
	protected final Logger log = LoggerFactory.getLogger(this.getClass());

	private final TProtocolFactory binaryProtocolFactory = new TBinaryProtocol.Factory();
	
	public abstract MessageDispatcher  getMessageDispatcher();
	
	protected SettableFuture<?> viewAccepted = SettableFuture.create();
				
	public Address getLocalAddress(){
		return getCluster().getAddress();
	}
		
	@SuppressWarnings("rawtypes")
	private <T> ListenableFuture<Map<Address, T>> _thriftCall(Collection<Address> dest, Collection<Address> exclusionList, boolean loopBack, int timeout, ResponseMode responseMode, InvocationInfo tInfo) throws TException{
		
		final Message msg = new Message();
		msg.setObject(new MessageWrapper(tInfo.buildCall(0, binaryProtocolFactory)));
		
		if (loopBack == false)
			msg.setTransientFlag(TransientFlag.DONT_LOOPBACK);
				
		final RequestOptions options = new RequestOptions(responseMode, timeout);
		
		if (exclusionList!=null){
			options.setExclusionList(exclusionList.toArray(new Address[exclusionList.size()]));
		}
		
		final SettableFuture<Map<Address, T>> f = SettableFuture.create();
		
		log.debug("call {}, dest={}, excl={}, loopback={}, timeout={}, respMode={}", tInfo.fullMethodName, dest, exclusionList, loopBack, timeout, responseMode);
		
		try {
			getMessageDispatcher().castMessageWithFuture(dest, msg, options, new FutureListener<RspList<MessageWrapper>>(){

				@Override
				public void futureDone(Future<RspList<MessageWrapper>> future) {
					
					log.debug("futureDone");

					RspList<MessageWrapper> resp;
					try {
						resp = future.get();
					} catch (InterruptedException | ExecutionException e1) {
						f.setException(e1);
						return;
					}
					
					log.trace("RspList:{}", resp);

					final Map<Address, T> ret = new HashMap<Address, T>();		
					
					for (Rsp<MessageWrapper>responce: resp){
						if (responce.getValue() !=null){
							
							try{
								
								final T success = (T)tInfo.setReply(responce.getValue().getTTransport(), binaryProtocolFactory);					
								ret.put(responce.getSender(), success);
								
							} catch (TApplicationException e) {
								if (e.getType() == TApplicationException.UNKNOWN_METHOD){
									log.debug("UNKNOWN_METHOD from {}:{}", responce.getSender(), e.getMessage());
								}else{
									log.error("Exception while reading thrift answer from " + responce.getSender(), e);
								}
							} catch (Exception e) {
								log.error("Exception while reading thrift answer from " + responce.getSender(), e);
							}
							
						}else{
							log.warn("null responce from {}", responce.getSender());
						}
					}
					
					f.set(ret);					
				}});
		} catch (Exception e) {
			f.setException(e);
		}
		
		return f;		
	}
		
	public <T> ListenableFuture<Map<Address, T>> call(Collection<Address> dest, Collection<Address> exclusionList, InvocationInfo tInfo, Options ... options) throws TException{
		
		Assert.notNull(tInfo, "tInfo must not be null");

		final SettableFuture<Map<Address, T>> ret = SettableFuture.create();
		
		Futures.addCallback(viewAccepted, new FutureCallback<Object>(){

			@Override
			public void onSuccess(Object result) {

				try {
					Futures.addCallback(_thriftCall(dest, exclusionList, isLoopback(options), getTimeout(options), getResponseMode(options), tInfo), new FutureCallback<Map<Address, T>>(){

						@Override
						public void onSuccess(Map<Address, T> result) {
							ret.set(result);						
						}

						@Override
						public void onFailure(Throwable t) {
							ret.setException(t);						
						}});
				} catch (TException e) {
					ret.setException(e);
				}
			}

			@Override
			public void onFailure(Throwable t) {
			}});
		
		return ret;
	}
	
}
