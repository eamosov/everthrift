package com.knockchat.appserver.transport;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TMessage;
import org.apache.thrift.protocol.TMessageType;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TMemoryBuffer;
import org.apache.thrift.transport.TMemoryInputTransport;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.transport.TTransportFactory;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketClose;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketConnect;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketError;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketMessage;
import org.eclipse.jetty.websocket.api.annotations.WebSocket;
import org.eclipse.jetty.websocket.client.ClientUpgradeRequest;
import org.eclipse.jetty.websocket.client.WebSocketClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Throwables;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.SettableFuture;
import com.knockchat.utils.thrift.ThriftInvocationHandler.InvocationInfo;

public class TWsJettyTransport extends TAsyncTransport {
	
	private static final Charset UTF_8 = Charset.forName("UTF-8");
	
	private static final Logger log = LoggerFactory.getLogger(TWsJettyTransport.class);
	
	
	static WebSocketClient client = new WebSocketClient();
	static {
		client.setDaemon(true);
		try {
			client.start();
		} catch (Exception e) {
			throw Throwables.propagate(e);
		}
	}

    private URI uri;
    private long timeout;
    
    private static final ByteBuffer EOF = ByteBuffer.wrap(new byte[1]);
    
    private final ThreadLocal<ByteArrayOutputStream> requestBuffer_ = new ThreadLocal<ByteArrayOutputStream>(){    	
    	@Override
    	protected ByteArrayOutputStream initialValue() {
    		 return new ByteArrayOutputStream();
    	}
    };
    
    private Websocket websocket;
    
    private final LinkedBlockingDeque<ByteBuffer> queue = new LinkedBlockingDeque<ByteBuffer>();
    private byte[] pendingBytes;
    private int pendingBytesPos;

    private final TProtocolFactory protocolFactory;
    private final TTransportFactory transportFactory;
    private final TProcessor processor;
    
    private SettableFuture<Session> connectFuture;
    private boolean isConnected = false;
    private boolean isOpened = false;
    private boolean closed = false;
    private final AsyncRegister async;
    
    private TransportEventsIF eventsHandler;    
    private final ExecutorService executor;
    
    //private Session session;
    
    /**
     * Нужно сохранять идентификаторы отправленных пакетов, чтобы вызвать исключения в случае потери соединения
     */
    private Set<Integer> pendingRequests = Sets.newHashSet();

    public TWsJettyTransport(URI uri, TProcessor processor, TProtocolFactory protocolFactory, TTransportFactory transportFactory, AsyncRegister async, ExecutorService executor) {
        this(uri, 3000, processor, protocolFactory, transportFactory, async, executor);
    }

    /**
     * 
     * @param uri
     * @param timeout  -  milliseconds
     * @param processor
     * @param protocolFactory
     * @param transportFactory
     * @param async
     * @param executor
     */
    public TWsJettyTransport(URI uri, long timeout, TProcessor processor, TProtocolFactory protocolFactory, TTransportFactory transportFactory, AsyncRegister async, ExecutorService executor) {
    	
    	//WebSocketImpl.DEBUG = true;    	
        this.uri = uri;
        this.timeout = timeout;
        this.processor = processor;
        this.protocolFactory = protocolFactory;
        this.transportFactory = transportFactory;
        this.async = async;
        this.executor = executor;        
    }

    @Override
    public synchronized boolean isOpen() {
        return isConnected;
    }
    
    @Override
    public void open() throws TTransportException {    	
    	openAsync();
    	waitForConnect();
    }
    
    public synchronized void openAsync() throws TTransportException {
    	
    	log.trace("openAsync()");
    	    		
		if (closed)
			throw new TTransportException(TTransportException.NOT_OPEN, "closed");
		
		if (isConnected || isOpened)
			throw new TTransportException(TTransportException.ALREADY_OPEN, "ALREADY_OPEN");
		
		isOpened = true;
		
		connectFuture = SettableFuture.create();

		websocket = new Websocket();
		final ClientUpgradeRequest request = new ClientUpgradeRequest();
		try {
			client.connect(websocket, uri, request);
		} catch (IOException e1) {
			throw new TTransportException(e1);
		}
		        
        executor.submit(new Runnable(){

			@Override
			public void run() {
		        try{
		        	final Session s = connectFuture.get(timeout, TimeUnit.MILLISECONDS);
		        	log.trace("Handshake completed, sessionId={}", s.toString());
		        } catch (TimeoutException | InterruptedException | ExecutionException e){
		        	log.debug("Wait for connect: {}", e.toString());
		        	fireOnConnectError();
		        	close();
		        }    	
				
			}});
    }
    
    public void waitForConnect() throws TTransportException{
        try{
        	final Session s = connectFuture.get(timeout, TimeUnit.MILLISECONDS);
        	log.trace("Handshake completed, websocket={}", s.toString());
        } catch (TimeoutException e){
        	log.debug("TimeoutException");
        	close();
        	throw new TTransportException(TTransportException.TIMED_OUT, "timed out");
        } catch (InterruptedException e) {
        	close();
        	throw new TTransportException(e);
		} catch (ExecutionException e) {
			close();
			if (e.getCause() !=null && e.getCause() instanceof TTransportException)
				throw (TTransportException)e.getCause();
			else
				throw new TTransportException(e);
		}    	
    }
    
    /**
     * Нельзя вызывать из потоков websocket, т.к. тут закрывается client
     */
    @Override
    public final void close() {
    	
    	log.trace("close()");
    	
    	final boolean s = connectFuture.setException(new TTransportException("connect error"));
    	
    	log.trace("settings exception for connectFuture: {}", s);
    	    	
    	final TransportEventsIF h;
    	
    	synchronized(this){
        	if (closed){
        		log.trace("allready closed");
        		return;
        	}
            	
        	closed = true;
            	                    
        	if (websocket != null){
//        		try {
        			websocket.close();
        			//websocket.awaitClose(Integer.MAX_VALUE, TimeUnit.SECONDS);
//        		} catch (InterruptedException e) {
//        			throw new RuntimeException(e);
//        		}                                    
        	}
        	
        	try {
				queue.put(EOF);
			} catch (InterruptedException e) {
				log.error("InterruptedException", e);
			}
        	            	    	
        	for (Integer i: pendingRequests){
        		log.debug("Throw TTransportException for call seqId={}", i);
        		final InvocationInfo<?> ii = async.pop(i);
        		if (ii !=null)
        			ii.setException(new TTransportException(TTransportException.END_OF_FILE, "closed"));
        	}
        	
        	pendingRequests.clear();        	        	
        	
        	if (isConnected){
        		isConnected = false;
        		
            	h = eventsHandler;
            }else{
            	h = null;
            }    		
    	}
    	    	
    	if (h!=null)
    		h.onClose();						

    }
        
	/**
	 * Без синхронизации во избежании возможности дедлока
	 */
    private void fireOnConnect(){
    	
    	log.trace("fireOnConnect() : {}", websocket.session);
    	    	
    	final TransportEventsIF h;
    	
    	synchronized(this){
    		if (isConnected == false && closed == false){
        		isConnected = true;

            	h = eventsHandler;
    		}else{
    			h = null;
    		}
    	}
    	
    	final boolean s = connectFuture.set(websocket.session.get());
    	
    	log.trace("fireOnConnect() connectFuture.set : {}", s);
    	
    	if (h!=null)
    		h.onConnect();    	    							    	
    }

    private void fireOnConnectError(){
    	
    	final TransportEventsIF h;
    	
    	synchronized(this){
    		h = eventsHandler;
    	}
    	
    	if (h!=null)
    		h.onConnectError();    	    	    	
    }
    
    @Override
    public int read(byte[] buf, final int off, final int len) throws TTransportException {
    	
    	if (len == 0)
    		return 0;
    	
    	synchronized(this){
        	if (async !=null)
        		throw new UnsupportedOperationException();    	
        	
        	if (!isConnected)
        		throw new TTransportException(TTransportException.NOT_OPEN, "closed");    		
    	}
    	
    	int read = 0;
    	
    	if (pendingBytes !=null){
    		final int pendingBytesLen = pendingBytes.length - pendingBytesPos;
    		final int l = pendingBytesLen <= len ? pendingBytesLen : len;
    		System.arraycopy(pendingBytes, pendingBytesPos, buf, off, l);
    		pendingBytesPos += l;
    		read += l;
    		
    		if (pendingBytesPos == pendingBytes.length){
    			pendingBytes = null;
    			pendingBytesPos = 0;
    		}
    		
    		if (read == len)
    			return len;
    	}
    	
    	while(read < len){
    		
    		ByteBuffer b;
			try {
				b = queue.poll(1000, TimeUnit.MILLISECONDS);
			} catch (InterruptedException e) {
				throw new TTransportException(e);
			}
    		
    		if (b == EOF)
    			throw new TTransportException("No more data available.");
    		
    		if (b == null){
    			synchronized(this){
    	        	if (!isConnected)
    	        		throw new TTransportException(TTransportException.NOT_OPEN, "closed");    		        				
    			}
    			continue;
    		}
    		
    		final byte bb[] = b.array();
    		final int bb_off = b.position();
    		final int bb_len = b.limit() - b.position();
    		
    		final int l = bb_len <= len - read ? bb_len : len - read;
    		System.arraycopy(bb, bb_off, buf, off + read, l);
    		read += l;
    		
    		if (l < bb_len){
    			pendingBytes = bb;
    			pendingBytesPos = bb_off + l;
    		}
    	}
    	
        return len;
    }

    @Override
    public void write(byte[] buf, int off, int len) throws TTransportException {
        requestBuffer_.get().write(buf, off, len);
    }

    public void flush(int seqId) throws TTransportException {
    	
    	synchronized(this){
    		if (async!=null)
    			pendingRequests.add(seqId);
    	}
   		
        flush();
    }

    @Override
    public void flush() throws TTransportException {
    	    	
    	final ByteArrayOutputStream rb = requestBuffer_.get(); 
        final byte[] data = rb.toByteArray();
        rb.reset();
        
        synchronized(this){
        	if (!isConnected)
        		throw new TTransportException(TTransportException.NOT_OPEN, "closed");        
            
        	final Session s = websocket.session.get();
        	
        	if (s == null)
        		throw new TTransportException(TTransportException.NOT_OPEN, "closed");
        	
            try {
            	s.getRemote().sendBytesByFuture(ByteBuffer.wrap(data)).get();
            } catch (InterruptedException | ExecutionException e) {
            	throw new TTransportException(e);
			}        	
        }        
    }
    
    private void onReadReply(TMessage msg, TTransport in, byte buf[], int offset, int length) throws IOException{
    	
    	final AsyncRegister async;
    	
    	synchronized(this){
    		async = this.async;
    	}
    	
    	if (async == null){
    		queue.add(ByteBuffer.wrap(buf, offset, length));
    	}else{
    		final InvocationInfo<?> ii = async.pop(msg.seqid);
    		if (ii==null){
    			log.warn("Callback for seqId={} not found", msg.seqid);
    		}else{    			
    			try {
    				synchronized(this){
        				pendingRequests.remove(msg.seqid);    					
    				}
					ii.setReply(in, protocolFactory);
				} catch (TException e) {

				}
    		}
    	}
    }

    private synchronized void onReadRequest(TMessage msg, TTransport inWrapT) throws TException{
    	
    	try(
    	    	final TMemoryBuffer outT = new TMemoryBuffer(1024);
    	    	final TTransport outWrapT = transportFactory.getTransport(outT);    			
    		){
    	
        	final TProtocol inP = protocolFactory.getProtocol(inWrapT);		
    		final TProtocol outP = protocolFactory.getProtocol(outWrapT);
    		
    		processor.process(inP, outP);
    		
            try {
            	
            	final Session s = websocket.session.get();
            	
            	if (s == null)
            		throw new TTransportException(TTransportException.NOT_OPEN, "closed");
            	
            	s.getRemote().sendBytesByFuture(outT.getByteBuffer()).get();
            } catch (Exception e) {
                new TTransportException(e);
            }    			    	    		
    	}    	    	
    }

    private void onRead(byte buf[], int offset, int length) throws TException, IOException{
        final TMemoryInputTransport inT = new TMemoryInputTransport(buf, offset, length);

    	try(
    	        final TTransport inWrapT = transportFactory.getTransport(inT);    			
    		){
    		
            final TMessage msg = protocolFactory.getProtocol(inWrapT).readMessageBegin();
            
            final TMemoryInputTransport copy = new TMemoryInputTransport(inWrapT.getBuffer(), 0, inWrapT.getBufferPosition() + inWrapT.getBytesRemainingInBuffer());
    		
    		if (msg.type == TMessageType.EXCEPTION || msg.type == TMessageType.REPLY){
    			onReadReply(msg, copy, buf, offset, length);
    		}else{
    			onReadRequest(msg, copy);
    		}    	    		
    	}        
    }
    

    @WebSocket(maxTextMessageSize = 64 * 1024, maxBinaryMessageSize = 64 * 1024)
    public class Websocket{
    	
    	private final CountDownLatch closeLatch;
    	
    	private final AtomicReference<Session> session = new AtomicReference<Session>();
    	
    	private Websocket(){
    		this.closeLatch = new CountDownLatch(1);    		
    	}
    	
    	public boolean awaitClose(int duration, TimeUnit unit) throws InterruptedException {
            return this.closeLatch.await(duration, unit);
        }
    	
    	public void close(){
    		final Session s = session.get();
    		if (s!=null)
    			s.close();
    	}
    	    	
    	@OnWebSocketConnect
    	public void onOpen(Session session) {
    		
    		this.session.set(session);
    		
    		log.trace("onOpen: {}", session.toString());
    		
    		executor.submit(new Runnable(){

				@Override
				public void run() {
					TWsJettyTransport.this.fireOnConnect();				
				}});
			
    	}
    	
    	@OnWebSocketMessage
    	public void onMessage( String message ){
    		final byte[] buf = message.getBytes(UTF_8);
    		
    		executor.execute(new Runnable(){

				@Override
				public void run() {
		    		try {
						onRead(buf, 0, buf.length);
					} catch (Exception e) {
						log.error("onMessage", e);
					}					
				}    			
    		});
    	}
    	
    	@OnWebSocketMessage
    	public void onMessage( final byte buf[], final int offset, final int length ) {
    		
    		executor.execute(new Runnable(){

				@Override
				public void run() {
		    		try {
						onRead(buf, offset, length);
					} catch (Exception e) {
						log.error("onMessage", e);
					}
				}
			});    			    		
    	}

    	@OnWebSocketError
    	public void onError( Throwable ex ) {
    		log.error("onError" , ex);
    		
    		executor.execute(new Runnable(){

				@Override
				public void run() {
					TWsJettyTransport.this.close();
				}});    		    		
    	}
    	
    	@OnWebSocketClose
    	public void onClose(int statusCode, String reason) {
        	log.trace("onClose, code={}, reason={}", statusCode, reason);
        	
        	this.session.set(null);
            this.closeLatch.countDown();        	
        	
    		executor.execute(new Runnable(){

				@Override
				public void run() {
					TWsJettyTransport.this.close();
				}});    		    		
    	}
    }

	public synchronized void setEventsHandler(TransportEventsIF eventsHandler) {
		this.eventsHandler = eventsHandler;
	}
}
