package com.knockchat.utils;

import java.lang.reflect.Proxy;
import java.net.URI;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.AutoExpandingBufferWriteTransport;
import org.apache.thrift.transport.THttpClient;
import org.apache.thrift.transport.TMemoryBuffer;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.transport.TTransportFactory;
import org.apache.thrift.transport.TZlibTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.knockchat.appserver.transport.AsyncRegister;
import com.knockchat.appserver.transport.TPersistWsTransport;
import com.knockchat.utils.thrift.ThriftInvocationHandler;
import com.knockchat.utils.thrift.ThriftInvocationHandler.InvocationCallback;
import com.knockchat.utils.thrift.ThriftInvocationHandler.InvocationInfo;
import com.knockchat.utils.thrift.ThriftInvocationHandler.NullResult;

public class ThriftContext {
	
	private static final Logger log = LoggerFactory.getLogger(ThriftContext.class);
	
	private ScheduledExecutorService scheduller;
	private ListeningExecutorService executor;
	private AsyncRegister async;
	private TPersistWsTransport tPersistWsTransport;
	private THttpClient tHttpTransport;
	
	private final AtomicInteger nThread = new AtomicInteger(0);
	
	private URI httpUri;
	private URI wsUri;
	private final TProcessor processor;
	private final TProtocolFactory protocolFactory = new TBinaryProtocol.Factory();
	private final TTransportFactory transportFactory = new TZlibTransport.Factory();
	
	private final ThreadLocal<InvocationInfo<?>> invocationInfo = new ThreadLocal<InvocationInfo<?>>();
		
	private long wsReconnectTimeout = 5000;
	private long wsConnectTimeoutMs = 5000;
	
	private long asyncCallTimeout = 5000;
	
	private boolean opened = false;
		
	public ThriftContext(URI httpUri, URI wsUri, TProcessor processor) {
		this.httpUri = httpUri;
		this.wsUri = wsUri;
		this.processor = processor;
	}
	
	public synchronized void start() throws TTransportException{
		
		scheduller = Executors.newSingleThreadScheduledExecutor(new ThreadFactory(){

			@Override
			public Thread newThread(Runnable r) {
				final Thread t = new Thread(r);
				t.setName("ThriftClientScheduller");
				t.setDaemon(true);
				return t;
			}});
		
		executor = MoreExecutors.listeningDecorator(new ThreadPoolExecutor(1, Integer.MAX_VALUE,
	            5L, TimeUnit.SECONDS,
	            new SynchronousQueue<Runnable>(),
	            new ThreadFactory(){
			
			@Override
			public Thread newThread(Runnable r) {
				final Thread t = new Thread(r);
				t.setName("ThriftClientExecutor-" + nThread.incrementAndGet());
				t.setDaemon(true);
				return t;
			}}));
				
		async = new AsyncRegister(MoreExecutors.listeningDecorator(scheduller));

		if (wsUri !=null)
			tPersistWsTransport =  new TPersistWsTransport(wsUri, processor, protocolFactory, transportFactory, async, scheduller, executor, wsReconnectTimeout, wsConnectTimeoutMs);
		
		if (httpUri !=null)
			tHttpTransport = new THttpClient(httpUri.toString());
		
		opened = true;
	}
	
	private void closeTHttpTransport(){
		if (tHttpTransport !=null){
			tHttpTransport.close();
			tHttpTransport = null;
		}		
	}
	
	private void closeTPersistWsTransport(){
		if (tPersistWsTransport !=null){
			tPersistWsTransport.close();
			tPersistWsTransport = null;
		}		
	}
	
	public synchronized void stop(){
		
		closeTPersistWsTransport();
		closeTHttpTransport();
		
		for (InvocationInfo ii: async.popAll()){
			ii.setException(new TTransportException(TTransportException.END_OF_FILE, "closed"));
		}
		
		async = null;
		
		scheduller.shutdown();
		scheduller = null;
		executor.shutdown();
		executor = null;

		opened = false;
	}
	
	private <R> R httpClientCall(InvocationInfo<R> ii, int seqId) throws TException{
		
		log.debug("httpClientCall: ii={}, seqId={}", ii, seqId);
		
		final TMemoryBuffer payload = ii.buildCall(seqId, new TBinaryProtocol.Factory());
		tHttpTransport.write(payload.getArray(), 0, payload.length());
		tHttpTransport.flush();
		final AutoExpandingBufferWriteTransport t = new AutoExpandingBufferWriteTransport(1024, 2);
		final byte[] tmpBuf = new byte[1024];
		int read;
		try{
			while ((read = tHttpTransport.read(tmpBuf, 0, tmpBuf.length))> 0){
				t.write(tmpBuf, 0, read);
			}
		}catch(TTransportException e){			
		}
		
		t.close();		
		return ii.setReply(t.getBuf().array(), 0, t.getPos(), protocolFactory);		
	}
	
	private <R> InvocationInfo<R> websocketCall(InvocationInfo<R> ii, int seqId, long tmMs) throws TTransportException{
		
		log.debug("websocketCall: ii={}, seqId={}", ii, seqId);
		
		final TMemoryBuffer payload = ii.buildCall(seqId, new TBinaryProtocol.Factory());
		
		final TTransport transportWrapper = transportFactory.getTransport(tPersistWsTransport);
		
		transportWrapper.write(payload.getArray(), 0, payload.length());		
		async.put(seqId, ii, tmMs);
		transportWrapper.flush();		
		return ii;					
	}
	
	@SuppressWarnings("unchecked")
	public <T> T service(Class<T> cls){
		return (T)Proxy.newProxyInstance(ThriftContext.class.getClassLoader(), new Class[]{cls}, new ThriftInvocationHandler(cls, new InvocationCallback(){

			@SuppressWarnings("rawtypes")
			@Override
			public Object call(InvocationInfo ii) throws NullResult, TException {
				
				if (!opened)
					throw new TTransportException(TTransportException.NOT_OPEN);
				
				final int seqId = async.nextSeqId();

				if (tPersistWsTransport !=null && tPersistWsTransport.isConnected()){
					try {
						return websocketCall(ii, seqId, asyncCallTimeout).get();
					} catch (InterruptedException | ExecutionException e) {
						throw new TTransportException(e);
					}
				}else if (tHttpTransport !=null){					
					return httpClientCall(ii, seqId);
				}else{
					throw new TTransportException(TTransportException.NOT_OPEN);
				}
			}}));		
	}
	
	@SuppressWarnings("unchecked")
	public <T> T asyncService(Class<T> cls){
		
		return (T)Proxy.newProxyInstance(ThriftContext.class.getClassLoader(), new Class[]{cls}, new ThriftInvocationHandler(cls, new InvocationCallback(){

			@SuppressWarnings("rawtypes")
			@Override
			public Object call(InvocationInfo ii) throws NullResult {
				invocationInfo.set(ii);
				throw new NullResult();
			}}));
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public <R> ListenableFuture<R> result(R unused, long tmMs) throws TTransportException{
		
		if (!opened)
			throw new TTransportException(TTransportException.NOT_OPEN);

		final InvocationInfo<R> ii = (InvocationInfo)invocationInfo.get();
		final int seqId = async.nextSeqId();
		
		if (tPersistWsTransport !=null && tPersistWsTransport.isConnected()){
			return websocketCall(ii, seqId, tmMs);
		}else if (tHttpTransport !=null){
			
			return executor.submit(new Callable<R>(){

				@Override
				public R call() throws Exception {
					return httpClientCall(ii, seqId);
				}});						
		}else{
			throw new TTransportException(TTransportException.NOT_OPEN);
		}						
	}
	
	public synchronized void connectWS() throws TTransportException{
		if (tPersistWsTransport!=null)
			tPersistWsTransport.open();
	}
	
	public synchronized void closeWS(){
		if (tPersistWsTransport!=null)
			tPersistWsTransport.close();
	}

	public URI getHttpUri() {
		return httpUri;
	}

	public synchronized void setHttpUri(URI httpUri) throws TTransportException {
		if (!Objects.equals(this.httpUri, httpUri)){
			closeTHttpTransport();		
			this.httpUri = httpUri;		
			if (opened && httpUri !=null)
				tHttpTransport = new THttpClient(httpUri.toString());			
		}
	}

	public URI getWsUri() {
		return wsUri;
	}

	/**
	 * websocket соединение после установки нового URI автоматически не открывается
	 * нужно вызвать connectWs()
	 * @param wsUri
	 */
	public synchronized void setWsUri(URI wsUri) {
		if (!Objects.equals(this.wsUri, wsUri)){
			closeTPersistWsTransport();
			this.wsUri = wsUri;
			if (opened && wsUri !=null)
				tPersistWsTransport =  new TPersistWsTransport(wsUri, processor, protocolFactory, transportFactory, async, scheduller, executor, wsReconnectTimeout, wsConnectTimeoutMs);			
		}
	}

	public long getWsReconnectTimeout() {
		return wsReconnectTimeout;
	}

	public void setWsReconnectTimeout(long wsReconnectTimeout) {
		this.wsReconnectTimeout = wsReconnectTimeout;
	}

	public long getWsConnectTimeoutMs() {
		return wsConnectTimeoutMs;
	}

	public void setWsConnectTimeoutMs(long wsConnectTimeoutMs) {
		this.wsConnectTimeoutMs = wsConnectTimeoutMs;
	}

}
