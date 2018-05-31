package org.everthrift.appserver.controller;

import com.google.common.util.concurrent.FutureCallback;
import org.apache.commons.lang.NotImplementedException;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TMessage;
import org.apache.thrift.protocol.TMessageType;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolException;
import org.apache.thrift.protocol.TProtocolUtil;
import org.apache.thrift.protocol.TType;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.everthrift.appserver.jgroups.RpcJGroups;
import org.everthrift.appserver.monitoring.RpsServletIF;
import org.everthrift.appserver.monitoring.RpsServletIF.DsName;
import org.everthrift.appserver.transport.http.RpcHttp;
import org.everthrift.appserver.transport.rabbit.RpcRabbit;
import org.everthrift.appserver.transport.tcp.RpcSyncTcp;
import org.everthrift.appserver.transport.websocket.RpcWebsocket;
import org.everthrift.appserver.utils.thrift.AbstractThriftClient;
import org.everthrift.appserver.utils.thrift.SessionIF;
import org.everthrift.appserver.utils.thrift.ThriftClient;
import org.everthrift.clustering.MessageWrapper;
import org.everthrift.clustering.thrift.ThriftControllerDiscovery;
import org.everthrift.thrift.TFunction;
import org.everthrift.thrift.ThriftCallFuture;
import org.everthrift.thrift.ThriftServicesDiscovery;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationContext;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * На каждый registry по экземпляру ThriftProcessor
 *
 * @author fluder
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class ThriftProcessor implements TProcessor {

    final public static Logger log = LoggerFactory.getLogger(ThriftProcessor.class);

    private final static Logger logControllerStart = LoggerFactory.getLogger("controller.start");

    private final static Logger logControllerEnd = LoggerFactory.getLogger("controller.end");

    public final Class<? extends Annotation> registryAnn;

    @Autowired(required = false)
    private RpsServletIF rpsServlet;

    @Autowired
    private ApplicationContext applicationContext;

    @Autowired
    private ThriftServicesDiscovery thriftServicesDb;

    @Autowired
    private ThriftControllerDiscovery thriftControllerDiscovery;

    @Autowired
    @Qualifier("listeningCallerRunsBoundQueueExecutor")
    private Executor executor;


    public ThriftProcessor(Class<? extends Annotation> registryAnn) {
        this.registryAnn = registryAnn;
    }

    public boolean processOnOpen(MessageWrapper in, ThriftClient thriftClient) {
        for (Class<? extends ConnectionStateHandler> cls : thriftControllerDiscovery.getConnectionStateHandlers(registryAnn
                                                                                                                    .getSimpleName())) {
            final ConnectionStateHandler h = applicationContext.getBean(cls);
            h.setup(in, thriftClient, registryAnn);
            if (!h.onOpen()) {
                return false;
            }
        }
        return true;
    }

    private void stat() {

        if (rpsServlet != null) {
            if (registryAnn == RpcSyncTcp.class) {
                rpsServlet.incThrift(DsName.THRIFT_TCP);
            } else if (registryAnn == RpcHttp.class) {
                rpsServlet.incThrift(DsName.THRIFT_HTTP);
            } else if (registryAnn == RpcJGroups.class) {
                rpsServlet.incThrift(DsName.THRIFT_JGROUPS);
            } else if (registryAnn == RpcRabbit.class) {
                rpsServlet.incThrift(DsName.THRIFT_RABBIT);
            } else if (registryAnn == RpcWebsocket.class) {
                rpsServlet.incThrift(DsName.THRIFT_WS);
            }
        }
    }

    @Nullable
    public <T> T process(@NotNull ThriftProtocolSupportIF<T> tps, ThriftClient thriftClient) throws TException {

        try {
            stat();

            final TMessage msg = tps.getTMessage();

            final LogEntry logEntry = new LogEntry(msg.name);
            logEntry.seqId = msg.seqid;


            final ThriftServicesDiscovery.ThriftMethodEntry thriftMethodEntry = thriftServicesDb.getByMethod(msg.name);

            if (thriftMethodEntry == null) {

                tps.skip();

                logNoController(thriftClient, msg.name, tps.getSessionId());
                return tps.serializeReply(new TApplicationException(TApplicationException.UNKNOWN_METHOD,
                                                                    "No controllerCls for method " + msg.name),
                                          null);
            }

            final TBase args;
            try {
                args = tps.deserializeArgs(thriftMethodEntry.makeArgs());
            } catch (Exception e) {
                return tps.serializeReply(e, thriftMethodEntry::makeResult);
            }

            final Class<? extends ThriftController> beanClass;
            final String beanName;

            final ThriftControllerInfo controllerInfo = thriftControllerDiscovery.getLocal(registryAnn.getSimpleName(), msg.name);

            if (controllerInfo != null) {
                beanClass = controllerInfo.beanClass;
                beanName = controllerInfo.getBeanName();
            } else {
                beanClass = ThriftController.class;
                beanName = registryAnn.getSimpleName() + "DefaultController";
            }

            final ThriftController controller;

            try {
                controller = applicationContext.getBean(beanName, beanClass);
            } catch (NoSuchBeanDefinitionException e) {
                logNoController(thriftClient, msg.name, tps.getSessionId());
                return tps.serializeReply(new TApplicationException(TApplicationException.UNKNOWN_METHOD,
                                                                    "No controllerCls for method " + msg.name),
                                          null);
            }

            final Logger log = LoggerFactory.getLogger(beanClass);
            logStart(log, thriftClient, msg.name, tps.getSessionId(), args);

            controller.setup(args, tps, logEntry, msg.seqid, thriftClient, registryAnn, tps.allowAsyncAnswer(), thriftMethodEntry.serviceName, thriftMethodEntry.methodName, executor, thriftMethodEntry);

            try {
                final Object ret = controller.handle(args);
                try {
                    return tps.serializeReply(ret, thriftMethodEntry::makeResult);
                } finally {
                    logEnd(log, controller, msg.name, tps.getSessionId(), ret);
                }
            } catch (AsyncAnswer e) {
                return null;
            } catch (Throwable e) {
                log.error("Exception while handle thrift request", e);
                try {
                    return tps.serializeReply(e, thriftMethodEntry::makeResult);
                } finally {
                    logEnd(log, controller, msg.name, tps.getSessionId(), e);
                }
            }
        } catch (RuntimeException e) {
            log.error("Exception while serving thrift request", e);
            throw e;
        }
    }

    @Override
    public boolean process(@NotNull TProtocol inp, @NotNull TProtocol out) throws TException {
        try {
            process(inp, out, new HashMap<>());
        } catch (RuntimeException e) {
        }

        return true;
    }

    /**
     * @param inp
     * @param out
     * @param attributes
     * @return Controller result (success or Exception)
     * @throws TException
     */
    @Nullable
    public Object process(@NotNull final TProtocol inp, @NotNull TProtocol out, @Nullable final Map<String, Object> attributes) throws TException {

        final TMessage msg = inp.readMessageBegin();

        final ThriftClient<Object> thriftClient = new AbstractThriftClient<Object>(null) {

            private SessionIF session;

            @Override
            public void setSession(SessionIF data) {
                session = data;
            }

            @Override
            public SessionIF getSession() {
                return session;
            }

            @Override
            public String getSessionId() {
                return null;
            }

            @Override
            public String getClientIp() {

                if (attributes != null && attributes.containsKey(MessageWrapper.HTTP_X_REAL_IP)) {
                    return (String) attributes.get(MessageWrapper.HTTP_X_REAL_IP);
                }

                TTransport inT = inp.getTransport();
                if (inT instanceof TFramedTransport) {
                    try {
                        Field f = TFramedTransport.class.getDeclaredField("transport_");
                        f.setAccessible(true);
                        inT = (TTransport) f.get(inT);
                        if (inT instanceof TSocket) {
                            return ((TSocket) inT).getSocket().getRemoteSocketAddress().toString();
                        }
                    } catch (@NotNull NoSuchFieldException | SecurityException | IllegalArgumentException | IllegalAccessException e) {
                        log.warn("cound't get remote address for transport of type {}", inT.getClass().getSimpleName());
                    }
                }
                return null;
            }

            @Override
            public void addCloseCallback(FutureCallback<Void> callback) {
            }

            @NotNull
            @Override
            protected CompletableFuture thriftCall(Object sessionId, int timeout, ThriftCallFuture tInfo) throws TException {
                throw new NotImplementedException();
            }

            @Override
            public boolean isThriftCallEnabled() {
                return false;
            }
        };

        final ThriftProtocolSupportIF s = new ThriftProtocolSupportIF<Object>() {

            @Override
            public String getSessionId() {
                return null;
            }

            @NotNull
            @Override
            public TMessage getTMessage() throws TException {
                return msg;
            }

            @NotNull
            @Override
            public Map<String, Object> getAttributes() {
                return attributes;
            }

            @NotNull
            @Override
            public <T extends TBase> T deserializeArgs(@NotNull final TBase args) throws TException {
                args.read(inp);
                inp.readMessageEnd();
                return (T) args;
            }

            @Override
            public void skip() throws TException {
                TProtocolUtil.skip(inp, TType.STRUCT);
                inp.readMessageEnd();
            }

            @NotNull
            private Object result(@NotNull TApplicationException o) {
                try {
                    out.writeMessageBegin(new TMessage(msg.name, TMessageType.EXCEPTION, msg.seqid));
                    ((TApplicationException) o).write(out);
                    out.writeMessageEnd();
                    out.getTransport().flush(msg.seqid);
                } catch (TException e) {
                    throw new RuntimeException(e);
                }

                return o;
            }

            @NotNull
            @Override
            public Object serializeReply(final Object successOrException, @NotNull final TFunction<Object, TBase> makeResult) {

                if (successOrException instanceof TApplicationException) {
                    return result((TApplicationException) successOrException);
                } else if (successOrException instanceof TProtocolException) {
                    return result(new TApplicationException(TApplicationException.PROTOCOL_ERROR, ((Exception) successOrException).getMessage()));
                } else if (successOrException instanceof Throwable && !(successOrException instanceof TException)) {
                    return result(new TApplicationException(TApplicationException.INTERNAL_ERROR, ((Throwable) successOrException).getMessage()));
                } else {
                    final TBase result;
                    try {
                        result = makeResult.apply(successOrException);
                    } catch (TApplicationException e) {
                        return result(e);
                    } catch (TException e) {
                        throw new RuntimeException(e);
                    }

                    try {
                        out.writeMessageBegin(new TMessage(msg.name, TMessageType.REPLY, msg.seqid));
                        result.write(out);
                        out.writeMessageEnd();
                        out.getTransport().flush(msg.seqid);
                    } catch (TException e) {
                        throw new RuntimeException(e);
                    }

                    return successOrException;
                }
            }

            @Override
            public void serializeReplyAsync(Object successOrException, @NotNull AbstractThriftController controller) {
                throw new NotImplementedException();
            }

            @Override
            public boolean allowAsyncAnswer() {
                return false;
            }
        };

        return process(s, thriftClient);
    }

    private static void logStart(@NotNull Logger l, @Nullable ThriftClient thriftClient, String method, String correlationId, @Nullable Object args) {
        if (l.isDebugEnabled() || logControllerStart.isDebugEnabled()) {
            final Logger _l = l.isDebugEnabled() ? l : logControllerStart;
            final String data = args == null ? null : args.toString();
            final SessionIF session = thriftClient != null ? thriftClient.getSession() : null;
            _l.debug("user:{} ip:{} START method:{} args:{} correlationId:{}", session != null ? session.getCredentials() : null,
                     thriftClient != null ? thriftClient.getClientIp() : null,
                     method, data != null
                             ? ((data.length() > 200 && !(l.isTraceEnabled() || logControllerEnd.isTraceEnabled()))
                                ? data.substring(0,
                                                 199)
                                    + "..."
                                : data)
                             : null,
                     correlationId);
        }
    }

    private static void logNoController(@Nullable ThriftClient thriftClient, String method, String correlationId) {
        if (log.isWarnEnabled() || logControllerStart.isWarnEnabled()) {
            final Logger _l = log.isWarnEnabled() ? log : logControllerStart;
            final SessionIF session = thriftClient != null ? thriftClient.getSession() : null;
            _l.warn("user:{} ip:{} No controllerCls for method:{} correlationId:{}", session != null ? session.getCredentials() : null,
                    thriftClient != null ? thriftClient.getClientIp() : null, method, correlationId);
        }
    }

    public static void logEnd(@NotNull Logger l, @NotNull AbstractThriftController c, String method, String correlationId, @Nullable Object ret) {

        if (l.isDebugEnabled() || logControllerEnd.isDebugEnabled()
            || (c.getExecutionMcs() > c.getWarnExecutionMcsLimit() && (l.isWarnEnabled() || logControllerEnd.isWarnEnabled()))) {

            final boolean tracing = l.isTraceEnabled() || logControllerEnd.isTraceEnabled();

            final String data = ret == null ? null
                                            : (tracing ? ret.toString() : (ret instanceof Exception ? ret.toString() : "<suppressed>"));
            final SessionIF session = c.thriftClient != null ? c.thriftClient.getSession() : null;
            final String format = "user:{} ip:{} END method:{} ctrl:{} delay:{} mcs correlationId: {} return: {}";
            final Object args[] = new Object[]{session != null ? session.getCredentials() : null,
                c.thriftClient != null ? c.thriftClient.getClientIp() : null, method, c.ctrlLog(),
                c.getExecutionMcs(), correlationId, data};

            final Logger _l;
            if (c.getExecutionMcs() > c.getWarnExecutionMcsLimit()) {
                _l = l.isWarnEnabled() ? l : logControllerEnd;
                _l.warn(format, args);
            } else {
                _l = l.isDebugEnabled() ? l : logControllerEnd;
                _l.debug(format, args);
            }
        }
    }

}
