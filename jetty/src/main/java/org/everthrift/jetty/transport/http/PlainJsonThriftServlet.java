package org.everthrift.jetty.transport.http;

import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.FutureCallback;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.NotImplementedException;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TMessage;
import org.apache.thrift.protocol.TMessageType;
import org.apache.thrift.protocol.TProtocolException;
import org.apache.thrift.transport.TMemoryBuffer;
import org.apache.thrift.transport.TTransportException;
import org.everthrift.appserver.controller.AbstractThriftController;
import org.everthrift.appserver.controller.ThriftControllerInfo;
import org.everthrift.appserver.controller.ThriftProcessor;
import org.everthrift.appserver.controller.ThriftProtocolSupportIF;
import org.everthrift.appserver.utils.thrift.AbstractThriftClient;
import org.everthrift.appserver.utils.thrift.GsonSerializer.TBaseSerializer;
import org.everthrift.appserver.utils.thrift.SessionIF;
import org.everthrift.clustering.MessageWrapper;
import org.everthrift.clustering.thrift.InvocationInfo;
import org.everthrift.utils.ClassUtils;
import org.everthrift.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;

import javax.annotation.PostConstruct;
import javax.servlet.AsyncContext;
import javax.servlet.ServletException;
import javax.servlet.ServletOutputStream;
import javax.servlet.WriteListener;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.beans.PropertyDescriptor;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class PlainJsonThriftServlet extends HttpServlet {

    private static final long serialVersionUID = 1L;

    private static final Logger log = LoggerFactory.getLogger(PlainJsonThriftServlet.class);

    private ThriftProcessor tp;

    @Autowired
    private ApplicationContext context;

    @Autowired
    private RpcHttpRegistry registry;

    private static final Gson gson = new GsonBuilder().setPrettyPrinting()
                                                      .disableHtmlEscaping()
                                                      .registerTypeHierarchyAdapter(TBase.class, new TBaseSerializer())
                                                      .create();

    @PostConstruct
    public void afterPropertiesSet() throws Exception {
        tp = ThriftProcessor.create(context, registry);
    }

    @Override
    protected void doOptions(HttpServletRequest req, HttpServletResponse response) throws ServletException, IOException {
        response.setHeader("Access-Control-Allow-Origin", "*");
        response.setHeader("Access-Control-Allow-Headers", "Content-Type, Access-Control-Allow-Headers, Authorization, X-Requested-With");
        super.doOptions(req, response);
    }

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
        doPost(request, response);
    }

    private void out(AsyncContext asyncContext, int status, String contentType, byte buf[]) throws IOException {
        out(asyncContext, status, contentType, buf, buf.length);
    }

    private void out(AsyncContext asyncContext, int status, String contentType, byte buf[], int length) throws IOException {

        ((HttpServletResponse) asyncContext.getResponse()).setStatus(status);
        asyncContext.getResponse().setContentType(contentType);

        final ServletOutputStream out = asyncContext.getResponse().getOutputStream();

        out.setWriteListener(new WriteListener() {

            @Override
            public void onWritePossible() throws IOException {

                if (out.isReady()) {
                    out.write(buf, 0, length);
                    asyncContext.complete();
                }
            }

            @Override
            public void onError(Throwable t) {

            }
        });
    }

    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws IOException {
        final AsyncContext asyncContext = request.startAsync();

        final String msgName = request.getPathInfo().substring(1);

        final Map<String, Object> attributes = Maps.newHashMap();
        attributes.put(MessageWrapper.HTTP_REQUEST_PARAMS, Optional.fromNullable(request.getParameterMap())
                                                                   .or(Collections.emptyMap()));
        attributes.put(MessageWrapper.HTTP_COOKIES, Optional.fromNullable(request.getCookies())
                                                            .or(() -> new Cookie[0]));
        attributes.put(MessageWrapper.HTTP_HEADERS,
                       Collections.list(request.getHeaderNames())
                                  .stream()
                                  .map(n -> Pair.create(n, request.getHeader(n)))
                                  .collect(Collectors.toMap(Pair::getFirst, Pair::getSecond)));

        try {
            final Pair<TMemoryBuffer, Integer> mw = tp.process(new ThriftProtocolSupportIF<Pair<TMemoryBuffer, Integer>>() {

                @Override
                public String getSessionId() {
                    return null;
                }

                @Override
                public TMessage getTMessage() throws TException {
                    return new TMessage(msgName, TMessageType.CALL, 0);
                }

                @Override
                public Map<String, Object> getAttributes() {
                    return attributes;
                }

                @Override
                public <T extends TBase> T readArgs(ThriftControllerInfo tInfo) throws TException {
                    final JsonParser jsonParser = new JsonParser();

                    final JsonObject _args;
                    final byte[] packet;
                    try {
                        packet = IOUtils.toByteArray(request.getInputStream());
                    } catch (IOException e1) {
                        throw new TException(e1);
                    }

                    final JsonElement e = jsonParser.parse(new String(packet, StandardCharsets.UTF_8));
                    if (e.isJsonNull()) {
                        _args = new JsonObject();
                    } else if (e.isJsonObject()) {
                        _args = e.getAsJsonObject();
                    } else {
                        throw new TProtocolException("POST data must be a JSON object");
                    }

                    for (Map.Entry<String, String[]> _e : request.getParameterMap().entrySet()) {
                        _args.add(_e.getKey(), jsonParser.parse(_e.getValue()[0]));
                    }

                    log.debug("method:{}, args:{}", msgName, _args);

                    final T ret = (T) gson.fromJson(_args, tInfo.getArgCls());

                    try {
                        final Method m = tInfo.getArgCls().getMethod("validate");
                        m.invoke(ret);
                    } catch (NoSuchMethodException | IllegalAccessException | IllegalArgumentException e1) {
                    } catch (InvocationTargetException e1) {
                        Throwables.propagateIfInstanceOf(e1.getCause(), TException.class);
                        throw Throwables.propagate(e1.getCause());
                    }

                    return ret;
                }

                @Override
                public void skip() throws TException {

                }

                private Pair<TMemoryBuffer, Integer> result(int code, String message, int httpStatusCode) {
                    final TMemoryBuffer outT = new TMemoryBuffer(1024);
                    final JsonObject ex = new JsonObject();
                    ex.addProperty("code", code);
                    ex.addProperty("message", message);
                    try {
                        outT.write(ex.toString().getBytes(StandardCharsets.UTF_8));
                    } catch (TTransportException e) {
                        throw new RuntimeException(e);
                    }
                    return Pair.create(outT, httpStatusCode);
                }

                @Override
                public Pair<TMemoryBuffer, Integer> result(final Object o, final ThriftControllerInfo tInfo) {

                    int httpCode = 200;

                    if (o instanceof TApplicationException) {
                        return result(((TApplicationException) o).getType(), ((TApplicationException) o).getMessage(), 400);

                    } else if (o instanceof TProtocolException) {
                        return result(TApplicationException.PROTOCOL_ERROR, ((Exception) o).getMessage(), 400);

                    } else if (o instanceof Exception) {

                        final Map<String, PropertyDescriptor> props = ClassUtils.getPropertyDescriptors(o.getClass());

                        httpCode = 400;

                        final PropertyDescriptor httpCodeDescr = props.get("httpCode");
                        if (httpCodeDescr != null) {
                            try {
                                httpCode = ((Number) httpCodeDescr.getReadMethod().invoke(o)).intValue();
                            } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
                            }
                        }

                        int code = -1;
                        final PropertyDescriptor codeDescr = props.get("code");
                        if (codeDescr != null) {
                            try {
                                code = ((Number) codeDescr.getReadMethod().invoke(o)).intValue();
                            } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
                            }
                        }

                        if (!(o instanceof TException)) {
                            return result(code, ((Exception) o).getMessage(), httpCode);
                        }
                    }

                    final TMemoryBuffer outT = new TMemoryBuffer(1024);
                    try {
                        outT.write(gson.toJson(o).getBytes(StandardCharsets.UTF_8));
                    } catch (TTransportException e) {
                        throw new RuntimeException(e);
                    }
                    return Pair.create(outT, httpCode);
                }

                @Override
                public void asyncResult(Object o, AbstractThriftController controller) {
                    final Pair<TMemoryBuffer, Integer> tt = result(o, controller.getInfo());
                    try {
                        out(asyncContext, tt.second, "application/json; charset=utf-8", tt.first.getArray(), tt.first.length());
                    } catch (IOException e) {
                        log.error("Async Error", e);
                    }

                    ThriftProcessor.logEnd(ThriftProcessor.log, controller, msgName, getSessionId(), o);
                }

                @Override
                public boolean allowAsyncAnswer() {
                    return true;
                }

            }, new AbstractThriftClient<Object>(null) {

                private SessionIF session;

                @Override
                public boolean isThriftCallEnabled() {
                    return false;
                }

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
                    final String xRealIp = request.getHeader(MessageWrapper.HTTP_X_REAL_IP);
                    if (xRealIp != null) {
                        return xRealIp;
                    } else {
                        return request.getRemoteHost() + ":" + request.getRemotePort();
                    }
                }

                @Override
                public void addCloseCallback(FutureCallback<Void> callback) {
                }

                @Override
                protected <T> CompletableFuture<T> thriftCall(Object sessionId, int timeout, InvocationInfo tInfo) throws TException {
                    throw new NotImplementedException();
                }
            });

            if (mw != null) {
                out(asyncContext, mw.second, "application/json; charset=utf-8", mw.first.getArray(), mw.first.length());
            }
        } catch (Exception e) {
            out(asyncContext, 500, "text/plain", e.getMessage().getBytes(StandardCharsets.UTF_8));
        }
    }
}
