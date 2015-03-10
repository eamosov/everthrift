package com.knockchat.utils;

import java.io.Closeable;
import java.lang.reflect.Proxy;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
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

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import com.knockchat.appserver.transport.AsyncRegister;
import com.knockchat.appserver.transport.TPersistWsTransport;
import com.knockchat.appserver.transport.TransportEventsIF;
import com.knockchat.utils.thrift.InvocationCallback;
import com.knockchat.utils.thrift.InvocationInfo;
import com.knockchat.utils.thrift.NullResult;
import com.knockchat.utils.thrift.ServiceAsyncIfaceProxy;
import com.knockchat.utils.thrift.ServiceIfaceProxy;

public class ThriftContext implements Closeable{
	
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
	
	private final Object wsIsConnectedLock = new Object();
	private boolean wsIsConnected = false;
	
	public static interface ThriftContextCallback{
		void call(ThriftContext tc) throws TException;
		void error(Throwable e);
	}
	
	private ThriftContextCallback onWsConnect;
	private ThriftContextCallback onWsDisconnect;
	
	private List<ThriftContextCallback> onConnectList = new ArrayList<ThriftContextCallback>();
	private List<SettableFuture<ThriftContext>> onConnectFutures = new ArrayList<SettableFuture<ThriftContext>>();
	
	private SettableFuture<ThriftContext> websocketConnectFuture;
	
	public static enum Transport{
		HTTP,
		WEBSOCKET,
		ANY
	}
	
	public ThriftContext(URI httpUri, URI wsUri) {
		this(httpUri, wsUri, null);
	}
	
	public ThriftContext(URI httpUri, URI wsUri, TProcessor processor) {
		this(httpUri, wsUri, processor, null, null);
	}
		
	public ThriftContext(URI httpUri, URI wsUri, TProcessor processor, ThriftContextCallback onWsConnect, ThriftContextCallback onWsDisconnect) {
		this.httpUri = httpUri;
		this.wsUri = wsUri;
		this.processor = processor;
		this.onWsConnect = onWsConnect;
		this.onWsDisconnect = onWsDisconnect;
	}
	
	public synchronized ListenableFuture<ThriftContext> open() throws TTransportException{
				
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
			createWebSocket();
		
		if (httpUri !=null)
			tHttpTransport = new THttpClient(httpUri.toString());
		
		opened = true;
		
		return wsUri == null ? null : openWebSocket();
	}
	
	private void destroyTHttp(){
		if (tHttpTransport !=null){
			tHttpTransport.close();
			tHttpTransport = null;
		}		
	}
	
	private void createWebSocket(){
		tPersistWsTransport =  new TPersistWsTransport(wsUri, processor, protocolFactory, transportFactory, async, scheduller, executor, wsReconnectTimeout, wsConnectTimeoutMs);
		tPersistWsTransport.setEventsHandler(new TransportEventsIF(){

			@Override
			public void onConnect() {
				final List<SettableFuture<ThriftContext>> sf;
				synchronized(wsIsConnectedLock){
					
					sf = new ArrayList<SettableFuture<ThriftContext>>(onConnectFutures.size());
					 
					wsIsConnected = true;
					
					if (onWsConnect !=null)
						try {
							onWsConnect.call(ThriftContext.this);
						} catch (TException e) {
							onWsConnect.error(e);
						}
					
					for(final ThriftContextCallback c : onConnectList)
						executor.submit(new Runnable(){
							@Override
							public void run() {
								try {
									c.call(ThriftContext.this);
								} catch (TException e) {
									c.error(e);
								}							
							}});
					
					sf.addAll(onConnectFutures);
					onConnectFutures.clear();
				}
				
				for (SettableFuture<ThriftContext> f : sf){
					f.set(ThriftContext.this);
				}				
			}

			@Override
			public void onClose() {
				synchronized(wsIsConnectedLock){
					wsIsConnected = false;
					
					if (onWsDisconnect !=null)
						try {
							onWsDisconnect.call(ThriftContext.this);
						} catch (TException e) {
							onWsDisconnect.error(e);
						}
				}
			}

			@Override
			public void onConnectError() {
				// TODO Auto-generated method stub
				
			}});
	}
	
	private void destroyWebsocket(){
		closeWebSocket();
		tPersistWsTransport = null;
	}
	
	@Override
	public synchronized void close(){
				
		destroyWebsocket();
		destroyTHttp();
		
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
	
	public <T> T onIface(Class<T> cls){
		return onIface(cls, Transport.ANY);
	}
	
	/**
	 * Подучение ссылку на сервис для синхронного вызова
	 * @param cls интерфейс сервиса XXXX.Iface
	 * @param use каким транспортом можно пользоваться
	 * @return сервис
	 */
	@SuppressWarnings("unchecked")
	public <T> T onIface(Class<T> cls, final Transport use){
		return (T)Proxy.newProxyInstance(ThriftContext.class.getClassLoader(), new Class[]{cls}, new ServiceIfaceProxy(cls, new InvocationCallback(){

			@SuppressWarnings("rawtypes")
			@Override
			public Object call(InvocationInfo ii) throws NullResult, TException {
				
				if (!opened)
					throw new TTransportException(TTransportException.NOT_OPEN);
				
				final int seqId = async.nextSeqId();

				if ((use == Transport.WEBSOCKET || use == Transport.ANY) && tPersistWsTransport !=null && tPersistWsTransport.isConnected()){
					try {
						return websocketCall(ii, seqId, asyncCallTimeout).get();
					} catch (InterruptedException | ExecutionException e) {
						throw new TTransportException(e);
					}
				}else if ((use == Transport.HTTP || use == Transport.ANY) && tHttpTransport !=null){					
					return httpClientCall(ii, seqId);
				}else{
					throw new TTransportException(TTransportException.NOT_OPEN);
				}
			}}));		
	}
	
	/**
	 * Подучение ссылку на сервис для aсинхронного вызова
	 * @param cls интерфейс сервиса XXXX.AsyncIface
	 * @param tmMs таймаут
	 * @param use каким транспортом можно пользоваться
	 * @return сервис
	 */	
	@SuppressWarnings("unchecked")
	public <T> T onAsyncIface(Class<T> cls, final long tmMs, final Transport use){
		return (T)Proxy.newProxyInstance(ThriftContext.class.getClassLoader(), new Class[]{cls}, new ServiceAsyncIfaceProxy(cls, new InvocationCallback(){

			@SuppressWarnings("rawtypes")
			@Override
			public Object call(final InvocationInfo ii) throws NullResult, TException {
				
				if (!opened)
					throw new TTransportException(TTransportException.NOT_OPEN);
				
				final int seqId = async.nextSeqId();
				
				final ListenableFuture<T> future;
				
				if ((use == Transport.WEBSOCKET || use == Transport.ANY) && tPersistWsTransport !=null && tPersistWsTransport.isConnected()){
					future =  websocketCall(ii, seqId, tmMs);
					
				}else if ((use == Transport.HTTP || use == Transport.ANY) && tHttpTransport !=null){
					
					future = executor.submit(new Callable<T>(){

						@Override
						public T call() throws Exception {
							return (T)httpClientCall(ii, seqId);
						}});						
				}else{
					throw new TTransportException(TTransportException.NOT_OPEN);
				}
				
				if (ii.asyncMethodCallback !=null)
					Futures.addCallback(future, new FutureCallback<T>(){

						@Override
						public void onSuccess(T result) {
							ii.asyncMethodCallback.onComplete(result);								
						}

						@Override
						public void onFailure(Throwable t) {
							ii.asyncMethodCallback.onError(t instanceof Exception ? (Exception)t : new Exception(t));
						}}, executor);				
				
				throw new NullResult();

			}}));		
		
	}
	
	/**
	 * Получение ссылку на сервис для асинхронных вызовов.
	 * Вызом любого метода такого сервиса регистрирует вызов и аргументы.
	 * Непосредственно для асинхронной отправки данных и получение ответа нужно вызвать метод result
	 * Удобно комбинировать два этих метода в одной строке, н-р:
	 * 
	 *   tc.result(rc.asyncService(AccountService.IFace.class).getMe(), 1000, Transport.ANY);
	 * 
	 * @param cls интерфейс сервиса
	 * @return сервис
	 */
	@SuppressWarnings("unchecked")
	public <T> T onIfaceAsAsync(Class<T> cls){
		
		return (T)Proxy.newProxyInstance(ThriftContext.class.getClassLoader(), new Class[]{cls}, new ServiceIfaceProxy(cls, new InvocationCallback(){

			@SuppressWarnings("rawtypes")
			@Override
			public Object call(InvocationInfo ii) throws NullResult {
				invocationInfo.set(ii);
				throw new NullResult();
			}}));
	}
	
	public <R> ListenableFuture<R> result(R unused) throws TTransportException{
		return result(unused, asyncCallTimeout, Transport.ANY);
	}
	
	/**
	 * Отправка на сервер данных и получение ответа (асинхронно) для асинхронного сервиса,
	 * полученного ранее через метод asyncService(Class cls).
	 * 
	 * @param unused	не используется, служет для удобной записи в одну строчку result(asyncService(..), ...)
	 * @param tmMs
	 * @param use
	 * @return
	 * @throws TTransportException
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public <R> ListenableFuture<R> result(R unused, long tmMs, Transport use) throws TTransportException{
		
		if (!opened)
			throw new TTransportException(TTransportException.NOT_OPEN);

		final InvocationInfo<R> ii = (InvocationInfo)invocationInfo.get();
		final int seqId = async.nextSeqId();
		
		if ((use == Transport.WEBSOCKET || use == Transport.ANY) && tPersistWsTransport !=null && tPersistWsTransport.isConnected()){
			return websocketCall(ii, seqId, tmMs);
		}else if ((use == Transport.HTTP || use == Transport.ANY) && tHttpTransport !=null){
			
			return executor.submit(new Callable<R>(){

				@Override
				public R call() throws Exception {
					return httpClientCall(ii, seqId);
				}});						
		}else{
			throw new TTransportException(TTransportException.NOT_OPEN);
		}						
	}
	
	public ListenableFuture<ThriftContext> openWebSocket() throws TTransportException{
		synchronized(this){
			if (tPersistWsTransport!=null){
				tPersistWsTransport.open();			
			}			
		}
		
		synchronized(wsIsConnectedLock){
			if (wsIsConnected){
				return Futures.immediateFuture(this);
			}else{
				final SettableFuture<ThriftContext> f = SettableFuture.create();
				onConnectFutures.add(f);
				return f;
			}
		}
	}
	
	public synchronized void closeWebSocket(){
		if (tPersistWsTransport!=null)
			tPersistWsTransport.close();
	}

	public URI getHttpUri() {
		return httpUri;
	}

	public synchronized void setHttpUri(URI httpUri) throws TTransportException {
		if (!Objects.equals(this.httpUri, httpUri)){
			destroyTHttp();		
			this.httpUri = httpUri;		
			if (opened && httpUri !=null)
				tHttpTransport = new THttpClient(httpUri.toString());			
		}
	}

	public URI getWsUri() {
		return wsUri;
	}

	/**
	 * @param wsUri
	 * @throws TTransportException 
	 */
	public synchronized ListenableFuture<ThriftContext> setWsUri(URI wsUri) throws TTransportException {
		if (!Objects.equals(this.wsUri, wsUri)){
			destroyWebsocket();
			this.wsUri = wsUri;
			if (opened && wsUri !=null){
				createWebSocket();
				return openWebSocket();
			}else{
				return null;
			}
		}else{
			return wsUri == null ? null : Futures.<ThriftContext>immediateFuture(this);
		}
	}

	public synchronized long getWsReconnectTimeout() {
		return wsReconnectTimeout;
	}

	public synchronized void setWsReconnectTimeout(long wsReconnectTimeout) {
		this.wsReconnectTimeout = wsReconnectTimeout;
	}

	public synchronized long getWsConnectTimeoutMs() {
		return wsConnectTimeoutMs;
	}

	public synchronized void setWsConnectTimeoutMs(long wsConnectTimeoutMs) {
		this.wsConnectTimeoutMs = wsConnectTimeoutMs;
	}

	public synchronized ThriftContextCallback getOnWsConnect() {
		return onWsConnect;
	}

	public synchronized void setOnWsConnect(ThriftContextCallback onWsConnect) {
		this.onWsConnect = onWsConnect;
	}

	public synchronized ThriftContextCallback getOnWsDisconnect() {
		return onWsDisconnect;
	}

	public synchronized void setOnWsDisconnect(ThriftContextCallback onWsDisconnect) {
		this.onWsDisconnect = onWsDisconnect;
	}
		
	/**
	 * Вызвать callback сразу после открытия websocket или немедленно, если webcoket уже открыт
	 * callback всегда вызывается асинхронно вызвавшему потоку
	 * @param callback
	 */
	public void onWsConnect(final ThriftContextCallback callback){
		
		synchronized(wsIsConnectedLock){
			if (wsIsConnected)
				executor.submit(new Runnable(){
					@Override
					public void run() {
						try {
							callback.call(ThriftContext.this);
						} catch (TException e) {
							callback.error(e);
						}							
					}});

			onConnectList.add(callback);			
		}
		
	}

}
