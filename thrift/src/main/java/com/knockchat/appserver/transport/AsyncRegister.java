package com.knockchat.appserver.transport;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.thrift.transport.TTransportException;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ListenableScheduledFuture;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.knockchat.utils.Pair;
import com.knockchat.utils.thrift.InvocationInfo;

public class AsyncRegister {
	
	private final Map<Integer, Pair<InvocationInfo, ListenableScheduledFuture>> callbacks = Maps.newHashMap();
	private final AtomicInteger seqId  = new AtomicInteger();
	private final ListeningScheduledExecutorService scheduller;

	public AsyncRegister(ListeningScheduledExecutorService scheduller) {
		this.scheduller = scheduller;
	}
	
	public synchronized InvocationInfo pop(int seqId){
		final Pair<InvocationInfo, ListenableScheduledFuture> p = callbacks.remove(seqId);
		
		if (p==null)
			return null;
		
		if (p.second !=null)
			p.second.cancel(false);
		
		return p.first.isDone() ? null : p.first;
	}
	
	public synchronized List<InvocationInfo> popAll(){
		final List<InvocationInfo> ret = Lists.newArrayList();
		
		for (Pair<InvocationInfo, ListenableScheduledFuture> p: callbacks.values()){

			if (p.second !=null)
				p.second.cancel(false);

			if (!p.first.isDone())
				ret.add(p.first);
		}
		callbacks.clear();
		return ret;
	}
	
	public synchronized void put(final int seqId, InvocationInfo ii, final long tmMs){
		callbacks.put(seqId, new Pair<InvocationInfo, ListenableScheduledFuture>(ii, scheduller.schedule(new Runnable(){

			public void run() {
				
				final Pair<InvocationInfo, ListenableScheduledFuture> p;
				
				synchronized(AsyncRegister.this){
					p = callbacks.remove(seqId);
				}
				
				p.first.setException(new TTransportException(TTransportException.TIMED_OUT, "TIMED_OUT"));
				
			}}, tmMs, TimeUnit.MILLISECONDS)));
	}

	public synchronized void put(int seqId, InvocationInfo ii){
		callbacks.put(seqId, new Pair<InvocationInfo, ListenableScheduledFuture>(ii, null));
	}

	public int nextSeqId(){
		return seqId.incrementAndGet();
	}

}
