package org.everthrift.jetty.transport.http;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;
import javax.servlet.AsyncContext;
import javax.servlet.ServletException;
import javax.servlet.ServletOutputStream;
import javax.servlet.WriteListener;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

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
import org.everthrift.appserver.utils.thrift.GsonSerializer.TBaseSerializer;
import org.everthrift.appserver.utils.thrift.AbstractThriftClient;
import org.everthrift.appserver.utils.thrift.SessionIF;
import org.everthrift.clustering.MessageWrapper;
import org.everthrift.clustering.thrift.InvocationInfo;
import org.everthrift.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;

import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class PlainJsonThriftServlet extends HttpServlet {

	private static final long serialVersionUID = 1L;
	
	private static final Logger log = LoggerFactory.getLogger(PlainJsonThriftServlet.class);
	
	private ThriftProcessor tp;
	
	@Autowired
	private ApplicationContext context;
	
	@Autowired
	private RpcHttpRegistry registry;
	
	private static final Gson gson =
			new GsonBuilder().
			setPrettyPrinting().
			disableHtmlEscaping().
			registerTypeHierarchyAdapter(TBase.class, new TBaseSerializer()).
			create();
	
	
	@PostConstruct
	public void afterPropertiesSet() throws Exception {
		tp =ThriftProcessor.create(context, registry);
	}

    protected void doOptions(HttpServletRequest req, HttpServletResponse response) throws ServletException, IOException {
		response.setHeader("Access-Control-Allow-Origin", "*");
		response.setHeader("Access-Control-Allow-Headers", "Content-Type, Access-Control-Allow-Headers, Authorization, X-Requested-With");
		super.doOptions(req, response);
    }
    
	@Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
		doPost(request, response);
	}
	
	private void out(AsyncContext asyncContext, HttpServletResponse response, byte buf[]) throws IOException{
		out(asyncContext, response, buf, buf.length);
	}
	
	private void out(AsyncContext asyncContext, HttpServletResponse response, byte buf[], int length) throws IOException{
		final ServletOutputStream out = response.getOutputStream();
		
		out.setWriteListener(new WriteListener(){
		    public void onWritePossible() throws IOException { 
		    	out.write(buf, 0, length);
		    	asyncContext.complete();
		    }
		    
		    public void onError(Throwable t){
		    	log.error("Async Error",t);
		    	asyncContext.complete();
		    }
		});		
	}

	@Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws IOException {
		
		final AsyncContext asyncContext = request.startAsync();
		
		final String msgName = request.getPathInfo().substring(1);
		
		final Map<String, Object> attributes = Maps.newHashMap();
		attributes.put(MessageWrapper.HTTP_REQUEST_PARAMS, Optional.fromNullable(request.getParameterMap()).or(Collections.emptyMap()));
		attributes.put(MessageWrapper.HTTP_COOKIES, Optional.fromNullable(request.getCookies()).or(() -> new Cookie[0]));
		attributes.put(MessageWrapper.HTTP_HEADERS, Collections.list(request.getHeaderNames()).stream().map( n -> Pair.create(n, request.getHeader(n))).collect(Collectors.toMap(Pair::getFirst, Pair::getSecond)));
		
		try {
			final TMemoryBuffer mw = tp.process(new ThriftProtocolSupportIF<TMemoryBuffer>(){

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
					
					final JsonElement e = jsonParser.parse(new String(packet));
					if (e.isJsonNull()){
						_args = new JsonObject();
					}else if (e.isJsonObject()){
						_args = e.getAsJsonObject();	
					}else{
						throw new TProtocolException("POST data must be a JSON object");
					}
										
					for (Map.Entry<String, String[]> _e: request.getParameterMap().entrySet()){
						_args.add(_e.getKey(), jsonParser.parse(_e.getValue()[0]));
					}
					
					log.debug("method:{}, args:{}", msgName, _args);
					
					final T ret =  (T)gson.fromJson(_args, tInfo.getArgCls());
					
					try{
						final Method m = tInfo.getArgCls().getMethod("validate");
						m.invoke(ret);
					} catch(NoSuchMethodException | IllegalAccessException | IllegalArgumentException e1){						
					} catch (InvocationTargetException e1) {
						Throwables.propagateIfInstanceOf(e1.getCause(), TException.class);
						throw Throwables.propagate(e1.getCause());
					}
					
					return ret;
				}

				@Override
				public void skip() throws TException {
					
				}

				private TMemoryBuffer result(TApplicationException o){
					final TMemoryBuffer outT = new TMemoryBuffer(1024);
					final JsonObject ex = new JsonObject();
					ex.addProperty("type", o.getType());
					ex.addProperty("message", o.getMessage());
					final JsonObject w = new JsonObject();
					w.add("error", ex);
					try {
						outT.write(w.toString().getBytes());
					} catch (TTransportException e) {
						throw new RuntimeException(e);
					}
					return outT;																			
				}
				
				@Override
				public TMemoryBuffer result(final Object o, final ThriftControllerInfo tInfo) {
					
					if (o instanceof TApplicationException){
						return result((TApplicationException)o);
					}else if (o instanceof TProtocolException) {
						return result(new TApplicationException(TApplicationException.PROTOCOL_ERROR, ((Exception)o).getMessage()));
					}else if (o instanceof Exception && !(o instanceof TException)){
						return result(new TApplicationException(TApplicationException.INTERNAL_ERROR, ((Exception)o).getMessage()));
					}else{
						final TBase result = tInfo.makeResult(o);
						final TMemoryBuffer outT = new TMemoryBuffer(1024);
						try {
							outT.write(gson.toJson(result).getBytes());
						} catch (TTransportException e) {
							throw new RuntimeException(e);
						}
						return outT;								
					}
				}

				@Override
				public void asyncResult(Object o, AbstractThriftController controller) {
					final TMemoryBuffer tt = result(o, controller.getInfo());
					try {
						out(asyncContext, response, tt.getArray(), tt.length());
					} catch (IOException e) {
						log.error("Async Error", e);
					}

					ThriftProcessor.logEnd(ThriftProcessor.log, controller, msgName, getSessionId(), o);					
				}

				@Override
				public boolean allowAsyncAnswer() {
					return true;
				}
				
			}, new AbstractThriftClient<Object>(null){
				
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
					if (xRealIp != null)
						return xRealIp;
					else
						return request.getRemoteHost() + ":" + request.getRemotePort();				
				}

				@Override
				public void addCloseCallback(FutureCallback<Void> callback) {
				}

				@Override
				protected <T> ListenableFuture<T> thriftCall(Object sessionId, int timeout, InvocationInfo tInfo) throws TException {
					throw new NotImplementedException();
				}});
			
			if (mw !=null){
				response.setStatus(200);
				response.setContentType("application/json");				
				out(asyncContext, response, mw.getArray(), mw.length());
			}
		} catch (Exception e) {
			response.setStatus(500);
			response.setContentType("text/plain");
			out(asyncContext, response, e.getMessage().getBytes());
		}				
	}    
}
