package org.everthrift.jetty.transport.websocket;

import com.google.common.collect.Maps;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TMessage;
import org.apache.thrift.protocol.TMessageType;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TMemoryBuffer;
import org.apache.thrift.transport.TMemoryInputTransport;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.transport.TTransportFactory;
import org.everthrift.appserver.controller.AbstractThriftController;
import org.everthrift.appserver.controller.DefaultTProtocolSupport;
import org.everthrift.appserver.controller.ThriftProcessor;
import org.everthrift.appserver.utils.thrift.AbstractThriftClient;
import org.everthrift.appserver.utils.thrift.SessionIF;
import org.everthrift.appserver.utils.thrift.ThriftClient;
import org.everthrift.appserver.utils.thrift.ThriftClientFactory;
import org.everthrift.clustering.MessageWrapper;
import org.everthrift.utils.AsyncRegister;
import org.everthrift.thrift.ThriftCallFuture;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.socket.BinaryMessage;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketHandler;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.adapter.jetty.JettyWebSocketSession;
import org.springframework.web.socket.handler.AbstractWebSocketHandler;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;

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
public class WebsocketThriftHandler extends AbstractWebSocketHandler implements WebSocketHandler, ThriftClientFactory {

    private static final Logger log = LoggerFactory.getLogger(WebsocketThriftHandler.class);

    private static final Charset UTF_8 = Charset.forName("UTF-8");

    public static final String UUID = "UUID";

    public static final String HTTP_X_REAL_IP = "X-Real-IP";

    private class SessionData {
        final WebSocketSession session;

        final AsyncRegister<ThriftCallFuture> async = new AsyncRegister<ThriftCallFuture>(listeningScheduledExecutorService);

        final SettableFuture<Void> closeFuture = SettableFuture.create();

        private AtomicReference<SessionIF> userSessionObject = new AtomicReference<SessionIF>();

        public SessionData(WebSocketSession session) {
            super();
            this.session = session;
        }
    }

    private ConcurrentMap<String, SessionData> sessionRegistry = Maps.newConcurrentMap();

    @Autowired
    private ListeningScheduledExecutorService listeningScheduledExecutorService;

    private ThriftProcessor thriftProcessor;

    private final TProtocolFactory protocolFactory;

    private final TTransportFactory transportFactory;

    private final boolean textOnly;

    public WebsocketThriftHandler(TTransportFactory transportFactory,
                                  final TProtocolFactory protocolFactory,
                                  final ThriftProcessor thriftProcessor,
                                  boolean textOnly) {

        this.transportFactory = transportFactory;
        this.protocolFactory = protocolFactory;
        this.thriftProcessor = thriftProcessor;
        this.textOnly = textOnly;
    }

    @Override
    protected void handleBinaryMessage(WebSocketSession session, BinaryMessage message) throws Exception {

        final byte[] payload = message.getPayload().array();

        if (log.isTraceEnabled()) {
            log.trace("handleBinaryMessage: size={}, content={}", payload.length, Arrays.toString(payload));
        }

        final TMemoryInputTransport orig = new TMemoryInputTransport(payload);

        try (final TTransport unwrapped = transportFactory.getTransport(orig);) {

            final TMessage msg = protocolFactory.getProtocol(unwrapped).readMessageBegin();

            log.trace("thrift message: {}", msg);

            final String sessionId = getSessionId(session);

            final TMemoryInputTransport copy = new TMemoryInputTransport(unwrapped.getBuffer(),
                                                                         0,
                                                                         unwrapped.getBufferPosition() + unwrapped.getBytesRemainingInBuffer());

            if (msg.type == TMessageType.EXCEPTION || msg.type == TMessageType.REPLY) {

                processThriftReply(session, msg, copy);
            } else {

                final MessageWrapper in = new MessageWrapper(copy).setHttpRequestParams((Map) session.getAttributes()
                                                                                                     .get(MessageWrapper.HTTP_REQUEST_PARAMS));

                final MessageWrapper out = thriftProcessor.process(new DefaultTProtocolSupport(sessionId, in, protocolFactory) {
                    @Override
                    public void asyncResult(final Object o, @NotNull final AbstractThriftController controller) {
                        writeBinary(session, (TMemoryBuffer) result(o, r -> controller.getInfo().thriftMethodEntry.makeResult(r))
                            .getTTransport());
                        ThriftProcessor.logEnd(ThriftProcessor.log, controller, msg.name, sessionId, o);
                    }
                }, getThriftClient(sessionId));

                if (out != null) {
                    writeBinary(session, (TMemoryBuffer) out.getTTransport());
                }
            }
        }
    }

    private void writeBinary(WebSocketSession session, TMemoryBuffer payload) {

        if (!transportFactory.getClass().equals(TTransportFactory.class)) {
            final TMemoryBuffer wrapped = new TMemoryBuffer(payload.length());
            try (final TTransport wrapper = transportFactory.getTransport(wrapped)) {
                try {
                    wrapper.write(payload.getArray(), 0, payload.length());
                    wrapper.flush();
                } catch (TTransportException e) {
                    throw new RuntimeException(e);
                }
                payload = wrapped;
            }
        }

        ((JettyWebSocketSession) session).getNativeSession()
                                         .getRemote()
                                         .sendBytesByFuture(payload.getByteBuffer());


    }

    @Override
    protected void handleTextMessage(WebSocketSession session, TextMessage message) throws Exception {

        if (log.isTraceEnabled()) {
            log.trace("handleTextMessage: size={}, content={}", message.getPayloadLength(), message.getPayload());
        }

        session.getAttributes().put("TEXT", true);

        final byte[] payload = message.asBytes();

        // Для текстовых сообщение не применяем архивирование
        final TTransport orig = new TMemoryInputTransport(payload);

        final TMessage msg = protocolFactory.getProtocol(orig).readMessageBegin();

        log.trace("thrift message: {}", msg);

        final String sessionId = getSessionId(session);

        final TMemoryInputTransport unwrapped = new TMemoryInputTransport(orig.getBuffer(), 0,
                                                                          orig.getBufferPosition() + orig.getBytesRemainingInBuffer());

        if (msg.type == TMessageType.EXCEPTION || msg.type == TMessageType.REPLY) {
            processThriftReply(session, msg, unwrapped);
        } else {
            final MessageWrapper in = new MessageWrapper(unwrapped).setHttpRequestParams((Map) session.getAttributes()
                                                                                                      .get(MessageWrapper.HTTP_REQUEST_PARAMS));

            final MessageWrapper out = thriftProcessor.process(new DefaultTProtocolSupport(sessionId, in, protocolFactory) {
                @Override
                public void asyncResult(final Object o, @NotNull final AbstractThriftController controller) {

                    final TMemoryBuffer payload = (TMemoryBuffer) result(o, r -> controller.getInfo().thriftMethodEntry.makeResult(r))
                        .getTTransport();

                    ((JettyWebSocketSession) session).getNativeSession().getRemote()
                                                     .sendStringByFuture(new String(payload.getArray(), 0, payload.length(), UTF_8));

                    ThriftProcessor.logEnd(ThriftProcessor.log, controller, msg.name, sessionId, o);
                }
            }, getThriftClient(sessionId));

            if (out != null) {
                final TMemoryBuffer payload2 = (TMemoryBuffer) out.getTTransport();

                ((JettyWebSocketSession) session).getNativeSession().getRemote()
                                                 .sendStringByFuture(new String(payload2.getArray(), 0, payload2.length(), UTF_8));
            }
        }
    }

    private void processThriftReply(WebSocketSession session, TMessage msg, TTransport in) {
        log.trace("process thrift reply message: {}", msg);

        final String sessionId = getSessionId(session);
        final SessionData sd = sessionRegistry.get(sessionId);
        if (sd == null) {
            log.error("No sessionData for session {}", sessionId);
            return;
        }
        final ThriftCallFuture tf = sd.async.pop(msg.seqid);
        if (tf == null) {
            log.warn("No registered thrift callback for msg:{}", msg);
            return;
        }

        try {
            tf.deserializeReply(in, protocolFactory);
        } catch (TException e) {
        }

        return;
    }

    @Override
    public void afterConnectionEstablished(WebSocketSession session) throws Exception {
        super.afterConnectionEstablished(session);

        final String sessionId = getSessionId(session);
        log.debug("Establish websocket connection: {}, attributes: {}", sessionId, session.getAttributes());

        final SessionData sd = new SessionData(session);
        sessionRegistry.put(sessionId, sd);

        final MessageWrapper mw = new MessageWrapper(null).setHttpRequestParams(
            (Map) session
                .getAttributes()
                .get(MessageWrapper.HTTP_REQUEST_PARAMS));

        if (thriftProcessor.processOnOpen(mw, thriftClient(sessionId, sd)) == false) {
            try {
                session.close(CloseStatus.POLICY_VIOLATION);
            } catch (IOException e) {

            }
        }
    }

    public String getSessionId(WebSocketSession session) {
        return (String) session.getAttributes().get(UUID);
    }

    @Override
    public void afterConnectionClosed(WebSocketSession session, CloseStatus status) throws Exception {
        super.afterConnectionClosed(session, status);

        final String sessionId = getSessionId(session);
        log.debug("Close websocket connection: {} {}", sessionId, status.toString());

        final SessionData sd = sessionRegistry.remove(sessionId);
        if (sd != null) {
            sd.closeFuture.set(null);

            for (ThriftCallFuture ii : sd.async.popAll()) {
                ii.completeExceptionally(new TTransportException(TTransportException.END_OF_FILE, "closed"));
            }
        }
    }

    public <T> CompletableFuture<T> thriftCall(String sessionId, int timeout, ThriftCallFuture tInfo) throws TException {

        final SessionData sd = sessionRegistry.get(sessionId);
        if (sd == null) {
            throw new TTransportException(TTransportException.NOT_OPEN, "websocket connection " + sessionId + " not found");
        }

        final int seqId = sd.async.nextSeqId();

        if (log.isTraceEnabled()) {
            log.trace("thriftCall: tInfo={}, seqId={}", tInfo, seqId);
        }

        sd.async.put(seqId, tInfo, timeout);
        try {
            final TMemoryBuffer payload = tInfo.serializeCall(seqId, protocolFactory);

            if (textOnly || sd.session.getAttributes().containsKey("TEXT")) {
                ((JettyWebSocketSession) sd.session).getNativeSession().getRemote()
                                                    .sendStringByFuture(new String(payload.getArray(), 0, payload.length(), UTF_8));
            } else {
                writeBinary(sd.session, payload);
            }

        } catch (Exception e) {
            sd.async.pop(seqId);
            throw e;
        }

        return tInfo;
    }

    private ThriftClient thriftClient(String sessionId, final SessionData sd) {
        return new AbstractThriftClient<String>(sessionId) {

            @Override
            protected <T> CompletableFuture<T> thriftCall(String sessionId, int timeout, ThriftCallFuture tInfo) throws TException {
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
                final String xRealIp = (String) sd.session.getAttributes().get(HTTP_X_REAL_IP);
                return xRealIp != null ? xRealIp : sd.session.getRemoteAddress().getAddress().getHostAddress();
            }
        };
    }

    @Override
    public ThriftClient getThriftClient(final String sessionId) {

        final SessionData sd = sessionRegistry.get(sessionId);
        if (sd == null) {
            log.warn("websocket connection {} not found", sessionId);
            return null;
        }

        return thriftClient(sessionId, sd);
    }
}
