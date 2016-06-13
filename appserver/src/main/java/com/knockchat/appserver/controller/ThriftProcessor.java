package com.knockchat.appserver.controller;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.Collections;
import java.util.Map;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationContext;
import org.springframework.core.task.AsyncTaskExecutor;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ListenableFuture;
import com.knockchat.appserver.jgroups.RpcJGroups;
import com.knockchat.appserver.monitoring.RpsServletIF;
import com.knockchat.appserver.monitoring.RpsServletIF.DsName;
import com.knockchat.appserver.transport.asynctcp.RpcAsyncTcp;
import com.knockchat.appserver.transport.http.RpcHttp;
import com.knockchat.appserver.transport.jms.RpcJms;
import com.knockchat.appserver.transport.tcp.RpcSyncTcp;
import com.knockchat.appserver.transport.websocket.RpcWebsocket;
import com.knockchat.clustering.MessageWrapper;
import com.knockchat.clustering.thrift.InvocationInfo;
import com.knockchat.utils.thrift.AbstractThriftClient;
import com.knockchat.utils.thrift.SessionIF;
import com.knockchat.utils.thrift.ThriftClient;

/**
 * На каждый registry по экземпляру ThriftProcessor
 * @author fluder
 *
 */
@SuppressWarnings({"rawtypes","unchecked"})
public class ThriftProcessor implements TProcessor{
	
	final public static Logger log = LoggerFactory.getLogger(ThriftProcessor.class);
	
	private final static Logger logControllerStart = LoggerFactory.getLogger("controller.start");
	private final static Logger logControllerEnd = LoggerFactory.getLogger("controller.end");
	
	private final ThriftControllerRegistry registry;
	
	@Qualifier("callerRunsBoundQueueExecutor")
	@Autowired
	private  AsyncTaskExecutor executor;
		
	@Autowired
	protected ApplicationContext applicationContext;
	
	@Autowired(required=false)
	private RpsServletIF rpsServlet;
		
	public static ThriftProcessor create(ApplicationContext context, ThriftControllerRegistry registry){
		return context.getBean(ThriftProcessor.class, registry);
	}
	
	public ThriftProcessor(ThriftControllerRegistry registry){
		this.registry = registry;
	}	
	
	public boolean processOnOpen(MessageWrapper in, ThriftClient thriftClient){	
		for (Class<ConnectionStateHandler> cls: registry.getStateHandlers()){
			final ConnectionStateHandler h = applicationContext.getBean(cls);
			h.setup(in, thriftClient, registry.getType());
			if (!h.onOpen())
				return false;
		}
		return true;
	}
	
	private void stat(){
		
		final Class<? extends Annotation> type = registry.getType();
		
		if (rpsServlet !=null){
			if (type == RpcSyncTcp.class || type == RpcAsyncTcp.class)
				rpsServlet.incThrift(DsName.THRIFT_TCP);
			else if (type == RpcHttp.class)
				rpsServlet.incThrift(DsName.THRIFT_HTTP);
			else if (type == RpcJGroups.class)
				rpsServlet.incThrift(DsName.THRIFT_JGROUPS);
			else if (type == RpcJms.class)
				rpsServlet.incThrift(DsName.THRIFT_JMS);
			else if (type == RpcWebsocket.class)
				rpsServlet.incThrift(DsName.THRIFT_WS);			
		}		
	}
	
	public <T> T process(ThriftProtocolSupportIF<T> tps, ThriftClient thriftClient) throws TException{
		
		try{
			stat();

			final TMessage msg = tps.getTMessage();
		
			final LogEntry logEntry = new LogEntry(msg.name);		
			logEntry.seqId = msg.seqid;
						
			final ThriftControllerInfo controllerInfo = registry.getController(msg.name);
			
			if (controllerInfo == null){
				tps.skip();
				
				logNoController(thriftClient, msg.name, tps.getSessionId());
				return tps.result(new TApplicationException( TApplicationException.UNKNOWN_METHOD, "No controllerCls for method " + msg.name), null);
			}
			
			final TBase args;
			try{
				args = tps.readArgs(controllerInfo);
			}catch(Exception e){
				return tps.result(e, controllerInfo);
			}
			
			final Logger log = LoggerFactory.getLogger(controllerInfo.getControllerCls());
			logStart(log, thriftClient, msg.name, tps.getSessionId(), args);									
			final ThriftController controller = controllerInfo.makeController(args, tps, logEntry, msg.seqid, thriftClient, registry.getType(), tps.allowAsyncAnswer());
			
			try{
				final Object ret = controller.handle(args);
				try{
					return tps.result(ret, controllerInfo);
				}finally{
					logEnd(log, controller, msg.name, tps.getSessionId(), ret);					
				}
			}catch(AsyncAnswer e){
				return null;
			}catch(Exception e){
				log.error("Exception while handle thrift request", e);
				try{
					return tps.result(e, controllerInfo);
				}finally{
					logEnd(log, controller, msg.name, tps.getSessionId(), null);					
				}
			}			
		}catch(RuntimeException e){
			log.error("Exception while serving thrift request", e);
			throw e;
		}
	}
	
	@Override
	public boolean process(TProtocol inp, TProtocol out) throws TException {
		try{
			process(inp, out, Collections.emptyMap());
		}catch(RuntimeException e){
		}
				
		return true;
	}
	
	/**
	 * 
	 * @param inp
	 * @param out
	 * @param attributes
	 * @return Controller result (success or Exception)
	 * @throws TException
	 */
	public Object process(final TProtocol inp, TProtocol out, final Map<String,Object> attributes) throws TException {
		
		final TMessage msg = inp.readMessageBegin();

		final ThriftClient<Object> thriftClient = new AbstractThriftClient<Object>(null){
			
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
				
				if (attributes !=null && attributes.containsKey(MessageWrapper.HTTP_X_REAL_IP))
					return (String)attributes.get(MessageWrapper.HTTP_X_REAL_IP);
				
				TTransport inT = inp.getTransport();
				if (inT instanceof TFramedTransport){
					try {
						Field f = TFramedTransport.class.getDeclaredField("transport_");
						f.setAccessible(true);
						inT = (TTransport)f.get(inT);
						if (inT instanceof TSocket){
							return ((TSocket) inT).getSocket().getRemoteSocketAddress().toString();
						}						
					} catch (NoSuchFieldException | SecurityException | IllegalArgumentException | IllegalAccessException e) {
						log.warn("cound't get remote address for transport of type {}", inT.getClass().getSimpleName());
					}
				}
				return null;
			}

			@Override
			public void addCloseCallback(FutureCallback<Void> callback) {				
			}

			@Override
			protected ListenableFuture thriftCall(Object sessionId,int timeout, InvocationInfo tInfo) throws TException {
				throw new NotImplementedException();
			}

			@Override
			public boolean isThriftCallEnabled() {
				return false;
			}
		};
		
		final ThriftProtocolSupportIF s = new ThriftProtocolSupportIF<Object>(){

			@Override
			public String getSessionId() {
				return null;
			}

			@Override
			public TMessage getTMessage() throws TException {
				return msg;
			}

			@Override
			public Map<String, Object> getAttributes() {
				return attributes;
			}

			@Override
			public <T extends TBase> T readArgs(final ThriftControllerInfo tInfo) throws TException{
				final TBase args = tInfo.makeArgument();
				args.read(inp);
				inp.readMessageEnd();
				return (T)args;
			}

			@Override
			public void skip() throws TException {
				TProtocolUtil.skip( inp, TType.STRUCT );
				inp.readMessageEnd();			
			}

			private Object result(TApplicationException o){
				try{
					out.writeMessageBegin( new TMessage( msg.name, TMessageType.EXCEPTION, msg.seqid));
					((TApplicationException)o).write(out);
					out.writeMessageEnd();
					out.getTransport().flush(msg.seqid);
				}catch (TException e){
					throw new RuntimeException(e);
				}
				
				return o;
			}
			
			@Override
			public Object result(final Object o, final ThriftControllerInfo tInfo) {
				
				if (o instanceof TApplicationException){
					return result((TApplicationException)o);
				}else if (o instanceof TProtocolException) {
					return result(new TApplicationException(TApplicationException.PROTOCOL_ERROR, ((Exception)o).getMessage()));
				}else if (o instanceof Exception && !(o instanceof TException)){
					return result(new TApplicationException(TApplicationException.INTERNAL_ERROR, ((Exception)o).getMessage()));
				}else{				
					final TBase result = tInfo.makeResult(o);
					
					try{
						out.writeMessageBegin( new TMessage( msg.name, TMessageType.REPLY, msg.seqid) );				
						result.write(out);
						out.writeMessageEnd();
						out.getTransport().flush(msg.seqid);
					}catch (TException e){
						throw new RuntimeException(e);
					}

					return o;	
				}
			}

			@Override
			public void asyncResult(Object o, AbstractThriftController controller) {
				throw new NotImplementedException();				
			}

			@Override
			public boolean allowAsyncAnswer() {
				return false;
			}};
			
			return process(s, thriftClient);
	}
	
	private static void logStart(Logger l, ThriftClient thriftClient, String method, String correlationId, Object args){		
		if(l.isDebugEnabled() || logControllerStart.isDebugEnabled()){
			final Logger _l = l.isDebugEnabled() ? l : logControllerStart;
			final String data = args == null ? null : args.toString();
			final SessionIF session = thriftClient !=null ? thriftClient.getSession() : null;
			_l.debug("user:{} ip:{} START method:{} args:{} correlationId:{}", session !=null ? session.getCredentials() : null, thriftClient !=null ? thriftClient.getClientIp() : null, method, data !=null ? ((data.length() > 200 && !(l.isTraceEnabled() || logControllerEnd.isTraceEnabled())) ? data.substring(0, 199) + "..." : data) : null, correlationId);
		}
	}
	
	private static void logNoController(ThriftClient thriftClient, String method, String correlationId){
		if(log.isWarnEnabled() || logControllerStart.isWarnEnabled()){
			final Logger _l = log.isWarnEnabled() ? log : logControllerStart;
			final SessionIF session = thriftClient !=null ? thriftClient.getSession() : null;
			_l.warn("user:{} ip:{} No controllerCls for method:{} correlationId:{}", session != null ? session.getCredentials() : null, thriftClient !=null ? thriftClient.getClientIp(): null, method, correlationId);
		}		
	}
	
	public static void logEnd(Logger l, AbstractThriftController c, String method, String correlationId, Object ret){
		
		if (l.isDebugEnabled() || logControllerEnd.isDebugEnabled() ||
				(c.getExecutionMcs() > c.getWarnExecutionMcsLimit() && (l.isWarnEnabled() || logControllerEnd.isWarnEnabled()))){
			
			final boolean tracing = l.isTraceEnabled() || logControllerEnd.isTraceEnabled();
						
			final String data = ret == null ? null : (tracing ? ret.toString() : (ret instanceof Exception ? ret.toString() : "<suppressed>"));
			final SessionIF session = c.thriftClient !=null ? c.thriftClient.getSession() : null;
			final String format = "user:{} ip:{} END method:{} ctrl:{} delay:{} mcs correlationId: {} return: {}";
			final Object args[] = new Object[]{session !=null ? session.getCredentials() : null, c.thriftClient !=null ? c.thriftClient.getClientIp() : null, method, c.ctrlLog(), c.getExecutionMcs(), correlationId, data};
			
			final Logger _l;
			if (c.getExecutionMcs() > c.getWarnExecutionMcsLimit()){
				_l = l.isWarnEnabled() ? l : logControllerEnd;
				_l.warn(format, args);
			}else{
				_l = l.isDebugEnabled() ? l : logControllerEnd;
				_l.debug(format, args);
			}						
		}		
	}
	
}
