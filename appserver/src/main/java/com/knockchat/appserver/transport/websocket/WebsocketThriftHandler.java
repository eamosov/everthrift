package com.knockchat.appserver.transport.websocket;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TMessage;
import org.apache.thrift.protocol.TMessageType;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TMemoryBuffer;
import org.apache.thrift.transport.TMemoryInputTransport;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.transport.TTransportFactory;
import org.eclipse.jetty.websocket.api.WebSocketException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageDeliveryException;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.MessagingException;
import org.springframework.messaging.SubscribableChannel;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.web.socket.BinaryMessage;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketHandler;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.adapter.jetty.JettyWebSocketSession;
import org.springframework.web.socket.handler.AbstractWebSocketHandler;

import com.google.common.collect.Maps;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.SettableFuture;
import com.knockchat.appserver.controller.MessageWrapper;
import com.knockchat.appserver.controller.MessageWrapper.WebsocketContentType;
import com.knockchat.appserver.controller.ThriftProcessor;
import com.knockchat.appserver.transport.AsyncRegister;
import com.knockchat.utils.thrift.AbstractThriftClient;
import com.knockchat.utils.thrift.InvocationInfo;
import com.knockchat.utils.thrift.SessionIF;
import com.knockchat.utils.thrift.ThriftClient;
import com.knockchat.utils.thrift.ThriftClientFactory;

/*
 * 
 * 1) Jetty слушает http-websocket и вызывает handleBinaryMessage в потоке Jetty
 * 2) В handleBinaryMessage сообщение отправляется в канал inWebsocketChannel
 * 3) В канале сообщения обрабатываются на Executor'е wsExecutor
 * 4) Ответ передается в DIRECT канал outWebsocketChannel   
 * 5) Из канала outWebsocketChannel сообщения отправляются обратно в Jetty
 * 
 * Т.о. входящий пакет читается в потоке Jetty а затем вся обработка происходит в wsExecutor
 * 
 * @author fluder
 *
 */
public class WebsocketThriftHandler extends AbstractWebSocketHandler implements WebSocketHandler, ThriftClientFactory, InitializingBean {
	
	private static final Logger log = LoggerFactory.getLogger(WebsocketThriftHandler.class);
	
	private static final Charset UTF_8 = Charset.forName("UTF-8");
	
	public static final String UUID = "UUID";
	public static final String HTTP_X_REAL_IP="X-Real-IP";
	
	private class SessionData{
		final WebSocketSession session;
		final AsyncRegister async = new AsyncRegister(listeningScheduledExecutorService);
		final SettableFuture<Void> closeFuture = SettableFuture.create();
		
		private AtomicReference<SessionIF> userSessionObject = new AtomicReference<SessionIF>();
				
		private WebsocketContentType lastContentType = WebsocketContentType.BINARY;
		
		public SessionData(WebSocketSession session) {
			super();
			this.session = session;
		}
	}
	
	private ConcurrentMap<String, SessionData> sessionRegistry = Maps.newConcurrentMap();
	
	@Autowired
	private ListeningScheduledExecutorService listeningScheduledExecutorService;
	
	@Autowired
	private ApplicationContext context;
	
	@Autowired
	private RpcWebsocketRegistry rpcWebsocketRegistry;
	
	private ThriftProcessor tp;
	
	private final SubscribableChannel inWebsocketChannel;	
	private final TProtocolFactory protocolFactory;
	
	private TTransportFactory transportFactory = new TTransportFactory();
	
	public WebsocketThriftHandler(final TProtocolFactory protocolFactory, final SubscribableChannel inWebsocketChannel, final SubscribableChannel outWebsocketChannel){
		this.protocolFactory = protocolFactory;
		this.inWebsocketChannel = inWebsocketChannel;
		
		outWebsocketChannel.subscribe(new MessageHandler(){

			@SuppressWarnings({ "rawtypes", "unchecked" })
			@Override
			public void handleMessage(Message<?> message) throws MessagingException {
				handleOut((Message)message);				
			}});
		
		this.inWebsocketChannel.subscribe(new MessageHandler(){

			@Override
			public void handleMessage(Message<?> message) throws MessagingException {
				final MessageWrapper payload = handleIn((Message)message, outWebsocketChannel);
				
				if (payload !=null){
					final GenericMessage<MessageWrapper> s = new GenericMessage<MessageWrapper>(payload.removeCorrelationHeaders(), message.getHeaders());
					outWebsocketChannel.send(s);
				}
			}});
	}
	
	@Override
	protected void handleBinaryMessage(WebSocketSession session, BinaryMessage message) throws Exception {
				
		final byte[] payload =  message.getPayload().array();
		
		if (log.isTraceEnabled())
			log.trace("handleBinaryMessage: size={}, content={}", payload.length, Arrays.toString(payload));
		
		final TMemoryInputTransport orig = new TMemoryInputTransport(payload);
				
		try(
				final TTransport unwrapped = transportFactory.getTransport(orig);
			){

			final TMessage msg = protocolFactory.getProtocol(unwrapped).readMessageBegin();
			
			log.trace("thrift message: {}", msg);
			
			final String sessionId = getSessionId(session);
			
			final SessionData sd = sessionRegistry.get(sessionId);
			if (sd!=null)
				sd.lastContentType = WebsocketContentType.BINARY;
			
			final TMemoryInputTransport copy = new TMemoryInputTransport(unwrapped.getBuffer(), 0, unwrapped.getBufferPosition() + unwrapped.getBytesRemainingInBuffer());
			
			if (msg.type == TMessageType.EXCEPTION || msg.type == TMessageType.REPLY){
				
				processThriftReply(session, msg, copy);
			}else{
				
				final Message<MessageWrapper> m = MessageBuilder.withPayload(new MessageWrapper(copy).setSessionId(sessionId).setWebsocketContentType(WebsocketContentType.BINARY).setHttpRequestParams((Map)session.getAttributes().get(MessageWrapper.HTTP_REQUEST_PARAMS))).build();		
				
				try{
					inWebsocketChannel.send(m);
				}catch(MessageDeliveryException e){
					log.warn("Reject websocket message, sessionId={}", this.getSessionId(session));
					session.close(CloseStatus.SERVICE_OVERLOAD);
				}				
			}								
		}
	
	}
	
	@Override
	protected void handleTextMessage(WebSocketSession session, TextMessage message) throws Exception {
		
		if (log.isTraceEnabled())
			log.trace("handleTextMessage: size={}, content={}", message.getPayloadLength(), message.getPayload());
		
		final byte[] payload =  message.asBytes();
		
		//Для текстовых сообщение не применяем архивирование
		final TTransport in = new TMemoryInputTransport(payload);
		
		final TMessage msg = protocolFactory.getProtocol(in).readMessageBegin();
		
		log.trace("thrift message: {}", msg);
		
		final String sessionId = getSessionId(session);
		final SessionData sd = sessionRegistry.get(sessionId);
		if (sd!=null)
			sd.lastContentType = WebsocketContentType.TEXT;
		
		final TMemoryInputTransport unwrapped = new TMemoryInputTransport(in.getBuffer(), 0, in.getBufferPosition() + in.getBytesRemainingInBuffer());
		
		if (msg.type == TMessageType.EXCEPTION || msg.type == TMessageType.REPLY){
			processThriftReply(session, msg, unwrapped);
		}else{
			final Message<MessageWrapper> m = MessageBuilder.withPayload(new MessageWrapper(unwrapped).setSessionId(sessionId).setWebsocketContentType(WebsocketContentType.TEXT).setHttpRequestParams((Map)session.getAttributes().get(MessageWrapper.HTTP_REQUEST_PARAMS))).build();
			
			try{
				inWebsocketChannel.send(m);
			}catch(MessageDeliveryException e){
				log.warn("Reject websocket message, sessionId={}", this.getSessionId(session));
				session.close(CloseStatus.SERVICE_OVERLOAD);
			}
		}						
	}	
	
	private void processThriftReply(WebSocketSession session, TMessage msg, TTransport in){
		log.trace("process thrift reply message: {}", msg);
		
		final String sessionId = getSessionId(session);
		final SessionData sd = sessionRegistry.get(sessionId);
		if (sd == null){
			log.error("No sessionData for session {}", sessionId);
			return;
		}
		final InvocationInfo tf = sd.async.pop(msg.seqid);
		if (tf == null){
			log.warn("No registered thrift callback for msg:{}", msg);
			return;
		}
		
		try {
			tf.setReply(in, protocolFactory);
		} catch (TException e) {
		}
		
		return;		
	}

	@Override
	public void afterConnectionEstablished(WebSocketSession session) throws Exception {
		super.afterConnectionEstablished(session);
		
		final String sessionId = getSessionId(session);
		log.debug("Establish websocket connection: {}, attributes: {}", sessionId, session.getAttributes());
	
		sessionRegistry.put(sessionId, new SessionData(session));
		
		final Message<MessageWrapper> m = MessageBuilder.withPayload(new MessageWrapper(null).setAttribute("onOpen", true).setSessionId(sessionId).setHttpRequestParams((Map)session.getAttributes().get(MessageWrapper.HTTP_REQUEST_PARAMS))).build();
		
		try{
			inWebsocketChannel.send(m);
		}catch(MessageDeliveryException e){
			log.warn("Reject websocket message, sessionId={}", this.getSessionId(session));
			session.close(CloseStatus.SERVICE_OVERLOAD);
		}		
	}
	
	public String getSessionId(WebSocketSession session){
		return (String)session.getAttributes().get(UUID);
	}
	
	@Override
	public void afterConnectionClosed(WebSocketSession session, CloseStatus status) throws Exception {
		super.afterConnectionClosed(session, status);
				
		final String sessionId = getSessionId(session);
		log.debug("Close websocket connection: {} {}", sessionId, status.toString());
		
		final SessionData sd = sessionRegistry.remove(sessionId);
		if (sd!=null){
			sd.closeFuture.set(null);
			
			for (InvocationInfo ii: sd.async.popAll()){
				ii.setException(new TTransportException(TTransportException.END_OF_FILE, "closed"));
			}
		}
	}
	
	public <T> ListenableFuture<T> thriftCall(String sessionId, int timeout, InvocationInfo tInfo) throws TException{
		
		final SessionData sd = sessionRegistry.get(sessionId);
		if (sd == null)
			throw new TTransportException(TTransportException.NOT_OPEN, "websocket connection " + sessionId + " not found");
	
		final int seqId = sd.async.nextSeqId();
		
		if (log.isTraceEnabled())
			log.trace("thriftCall: tInfo={}, seqId={}", tInfo, seqId);
		
		sd.async.put(seqId, tInfo, timeout);
		try{
			write(sd, sd.lastContentType, tInfo.buildCall(seqId, protocolFactory));
		}catch(TException e){
			sd.async.pop(seqId);
			throw e;
		}
		
		return tInfo;
	}

	@Override
	public ThriftClient getThriftClient(final String sessionId) {
		
		final SessionData sd = sessionRegistry.get(sessionId);
		if (sd == null){
			log.warn("websocket connection {} not found", sessionId);
			return null;
		}
		
		return new AbstractThriftClient<String>(sessionId){
			
			@Override
			protected <T> ListenableFuture<T> thriftCall(String sessionId, int timeout, InvocationInfo tInfo) throws TException {
				return WebsocketThriftHandler.this.thriftCall(sessionId, timeout, tInfo);
			}

			@Override
			public boolean isThriftCallEnabled() {
				return true;
			}
			
			@Override
			public void setSession(SessionIF data) {				
				sd.userSessionObject.set(data);				
			}

			@Override
			public SessionIF getSession() {
				return sd.userSessionObject.get();				
			}

			@Override
			public String getSessionId() {
				return sessionId;
			}

			@Override
			public void addCloseCallback(FutureCallback<Void> callback) {
				Futures.addCallback(sd.closeFuture, callback);
			}

			@Override
			public String getClientIp() {
				final String xRealIp =  (String)sd.session.getAttributes().get(HTTP_X_REAL_IP);
				return xRealIp != null ? xRealIp : sd.session.getRemoteAddress().getAddress().getHostAddress();
			}
		};
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		tp =ThriftProcessor.create(context, rpcWebsocketRegistry, protocolFactory);
	}
		
	private MessageWrapper handleIn(Message<MessageWrapper> m, MessageChannel outChannel){

		final MessageWrapper w = m.getPayload().setMessageHeaders(m.getHeaders()).setOutChannel(outChannel);
		
		final String sessionId = w.getSessionId();

		log.debug("handleIn: {}, adapter={}, processor={}, sessionId={}", new Object[]{m, this, tp, sessionId});
				
		if (sessionId == null){
			log.error("websocket sessionId is null for message: {}", m);
			return null;
		}
		
		final ThriftClient tc = getThriftClient(sessionId);

		if (w.getAttribute("onOpen") !=null){
			if (tp.processOnOpen(w, tc) == false){
				final SessionData sd = sessionRegistry.get(sessionId);
				if (sd !=null)
					try {
						sd.session.close(CloseStatus.POLICY_VIOLATION);
					} catch (IOException e) {
					}
			}
			return null;
		}else{
			try {
				return tp.process(w, tc);
			} catch (Exception e) {
				log.error("Exception while execution thrift processor:", e);
				return null;
			}			
		}		
	}
	
	private void write(SessionData sd, WebsocketContentType contentType, TMemoryBuffer payload) throws TTransportException{
		try{
			if (contentType == null || contentType == WebsocketContentType.BINARY){
				
				if (!transportFactory.getClass().equals(TTransportFactory.class)){
					final TMemoryBuffer wrapped = new TMemoryBuffer(payload.length());
					try(
							final TTransport wrapper =  transportFactory.getTransport(wrapped);
						){
						
						wrapper.write(payload.getArray(), 0, payload.length());
						wrapper.flush();
						payload = wrapped;
					}
				}					
				
				((JettyWebSocketSession)sd.session).getNativeSession().getRemote().sendBytesByFuture(payload.getByteBuffer());
				
			}else if (contentType == WebsocketContentType.TEXT){
				
				((JettyWebSocketSession)sd.session).getNativeSession().getRemote().sendStringByFuture(new String(payload.getArray(), 0, payload.length(), UTF_8));
				
			}else{
				log.error("Invalid CONTENT_TYPE:{}", contentType);
			}
		}catch(WebSocketException e){
			log.debug("WebSocketException", e);
			throw new TTransportException(e);
		}catch(RuntimeException e){
			log.error("Unknown exception while send data to websocket: ", e);
			throw e;
		}
		
	}
	
	private void handleOut(Message<MessageWrapper> m) {
		
		log.debug("handleOut: {}, adapter={}, processor={}", new Object[]{m, this, tp});
		
		final String sessionId = (String)m.getPayload().getSessionId();
		
		if (sessionId == null){
			log.error("sessionId is NULL, message={}", m);
			return;
		}

		final SessionData sd = sessionRegistry.get(sessionId);
		if (sd == null){
			log.debug("websocket connection " + sessionId + " not found");
			return;
		}
		
		try {
			write(sd, m.getPayload().getWebsocketContentType(), (TMemoryBuffer)m.getPayload().getTTransport());
		} catch (TTransportException e) {
		}
	}

	public TTransportFactory getTransportFactory() {
		return transportFactory;
	}

	public void setTransportFactory(TTransportFactory transportFactory) {
		this.transportFactory = transportFactory;
	}	
	
}
