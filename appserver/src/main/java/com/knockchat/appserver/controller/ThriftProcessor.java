package com.knockchat.appserver.controller;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;

import org.apache.commons.lang.NotImplementedException;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TMessage;
import org.apache.thrift.protocol.TMessageType;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.protocol.TProtocolUtil;
import org.apache.thrift.protocol.TType;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TMemoryBuffer;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.jgroups.Address;
import org.jgroups.blocks.ResponseMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationContext;
import org.springframework.core.task.AsyncTaskExecutor;

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ListenableFuture;
import com.knockchat.appserver.cluster.MulticastThriftTransport;
import com.knockchat.appserver.monitoring.RpsServlet;
import com.knockchat.appserver.monitoring.RpsServlet.DsName;
import com.knockchat.appserver.transport.asynctcp.RpcAsyncTcp;
import com.knockchat.appserver.transport.http.RpcHttp;
import com.knockchat.appserver.transport.jgroups.RpcJGroups;
import com.knockchat.appserver.transport.jms.RpcJms;
import com.knockchat.appserver.transport.tcp.RpcSyncTcp;
import com.knockchat.appserver.transport.websocket.RpcWebsocket;
import com.knockchat.utils.thrift.AbstractThriftClient;
import com.knockchat.utils.thrift.InvocationInfo;
import com.knockchat.utils.thrift.SessionIF;
import com.knockchat.utils.thrift.ThriftClient;

/**
 * На каждый registry по экземпляру ThriftProcessor
 * @author fluder
 *
 */
@SuppressWarnings({"rawtypes","unchecked"})
public class ThriftProcessor implements TProcessor{
	
	private final static Logger log = LoggerFactory.getLogger(ThriftProcessor.class);
	
	private final static Logger logControllerStart = LoggerFactory.getLogger("controller.start");
	private final static Logger logControllerEnd = LoggerFactory.getLogger("controller.end");
	
	private final ThriftControllerRegistry registry;
	
	@Qualifier("callerRunsBoundQueueExecutor")
	@Autowired
	private  AsyncTaskExecutor executor;
		
	@Autowired
	protected ApplicationContext applicationContext;
	
	@Autowired
	private RpsServlet rpsServlet;
	
	private final TProtocolFactory protocolFactory;
	
	public static ThriftProcessor create(ApplicationContext context, ThriftControllerRegistry registry, TProtocolFactory protocolFactory){
		return context.getBean(ThriftProcessor.class, registry, protocolFactory);
	}
	
	public ThriftProcessor(ThriftControllerRegistry registry, TProtocolFactory protocolFactory){
		this.registry = registry;
		this.protocolFactory = protocolFactory;
	}	
	
	public boolean processOnOpen(MessageWrapper in, ThriftClient thriftClient){	
		for (Class<ConnectionStateHandler> cls: registry.getStateHandlers()){
			final ConnectionStateHandler h = applicationContext.getBean(cls);
			h.setup(in, thriftClient, registry.getType(), protocolFactory);
			if (!h.onOpen())
				return false;
		}
		return true;
	}
	
	private void stat(){
		
		final Class<? extends Annotation> type = registry.getType();
		
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
					
	public MessageWrapper process(MessageWrapper in, ThriftClient thriftClient) throws TException{
		
		try{
			stat();

			final TProtocol inp = protocolFactory.getProtocol(in.getTTransport());
			final TMessage msg = inp.readMessageBegin();
		
			final LogEntry logEntry = new LogEntry(msg.name);		
			logEntry.seqId = msg.seqid;
						
			final ThriftControllerInfo controllerInfo = registry.getController(msg.name);
			
			if (controllerInfo == null){
				TProtocolUtil.skip( inp, TType.STRUCT );
				inp.readMessageEnd();
				
				logNoController(thriftClient, msg.name, in.getSessionId());
				
				final TMemoryBuffer outT = new TMemoryBuffer(1024);									
				final TProtocol out = protocolFactory.getProtocol(outT);
				final TApplicationException x = new TApplicationException( TApplicationException.UNKNOWN_METHOD, "No controllerCls for method " + msg.name);
				out.writeMessageBegin( new TMessage( msg.name, TMessageType.EXCEPTION, msg.seqid));
				x.write(out);
				out.writeMessageEnd();
				out.getTransport().flush(msg.seqid);											
				return new MessageWrapper(outT).copyAttributes(in).removeCorrelationHeaders();
			}
			
			final TBase args = controllerInfo.makeArgument();
			args.read(inp);
			inp.readMessageEnd();
			
			final Logger log = LoggerFactory.getLogger(controllerInfo.getControllerCls());
			logStart(log, thriftClient, msg.name, in.getSessionId(), args);									
			final ThriftController controller = controllerInfo.makeController(args, new MessageWrapper(null).copyAttributes(in), logEntry, msg.seqid, thriftClient, registry.getType(), this.protocolFactory, true);
			
			try{
				final Object ret = controller.handle(args);
				final TMemoryBuffer outT = controller.serializeAnswer(ret);
				controller.setEndNanos(System.nanoTime());
				controller.setResultSentFlag();
				logEnd(log, controller, msg.name, in.getSessionId(), ret);
				return  new MessageWrapper(outT).copyAttributes(in).removeCorrelationHeaders();				
			}catch(AsyncAnswer e){
				return null;
			}catch(Exception e){
				log.error("Exception while handle thrift request", e);
				final TMemoryBuffer outT = controller.serializeAnswer(new TApplicationException(TApplicationException.INTERNAL_ERROR));
				controller.setEndNanos(System.nanoTime());
				controller.setResultSentFlag();
				logEnd(log, controller, msg.name, in.getSessionId(), null);				
				return  new MessageWrapper(outT).copyAttributes(in).removeCorrelationHeaders();
			}			
		}catch(RuntimeException e){
			log.error("Exception while serving thrift request", e);
			throw e;
		}
	}
	
	private void castThriftMessage(final RpcClustered ann, final ThriftControllerInfo ctrl, final TMessage msg, final TBase args){
		final InvocationInfo ii;
		try {
			ii = new InvocationInfo(msg.name, args, ctrl.getControllerCls().getConstructor());
		} catch (NoSuchMethodException | SecurityException e) {
			throw Throwables.propagate(e);
		}
		
		ListenableFuture<Map<Address, Object>> clusterResults;
		try {
			clusterResults = applicationContext.getBean(MulticastThriftTransport.class).thriftCall(false, ann.timeout(), msg.seqid, ann.value(), ii);
		} catch (BeansException | TException e) {
			throw Throwables.propagate(e);
		}
			
		if (ann.value() == ResponseMode.GET_NONE){
			return;
		}
		
		try {
			clusterResults.get();
		} catch (InterruptedException | ExecutionException e) {
			throw Throwables.propagate(e);
		}
	}

	@Override
	public boolean process(TProtocol inp, TProtocol out) throws TException {
		try{
			process(inp, out, null);
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
	public Object process(final TProtocol inp, TProtocol out, final MessageWrapper attributes) throws TException {
		
		stat();
		
		final TMessage msg = inp.readMessageBegin();

		final ThriftControllerInfo controllerInfo = registry.getController(msg.name);
		
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
				
				if (attributes !=null)
					return (String)attributes.getAttribute(MessageWrapper.HTTP_X_REAL_IP);
				
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

		try{
			
			if (controllerInfo == null){
				TProtocolUtil.skip( inp, TType.STRUCT );
				inp.readMessageEnd();
				
				logNoController(thriftClient, msg.name, null);
								
				final TApplicationException x = new TApplicationException( TApplicationException.UNKNOWN_METHOD, "No controllerCls for method " + msg.name);
				out.writeMessageBegin( new TMessage( msg.name, TMessageType.EXCEPTION, msg.seqid));
				x.write(out);
				out.writeMessageEnd();
				out.getTransport().flush(msg.seqid);							
				return x;
			}
			
			final TBase args = controllerInfo.makeArgument();
			args.read(inp);
			inp.readMessageEnd();
			
			final Logger _log = LoggerFactory.getLogger(controllerInfo.getControllerCls());
			
			logStart(_log, thriftClient, msg.name, null, args);
			
			final LogEntry logEntry = new LogEntry(msg.name);
			
			final ThriftController controller = controllerInfo.makeController(args, attributes, logEntry, msg.seqid, thriftClient, registry.getType(), this.protocolFactory, false);			
			
			/*
			 * TODO т.к. текущий метов обрабатывает только @RpcSyncTcp контроллеры, то циклический вызовов не получится,
			 * однако это решение не полностью универсально.
			 * Необходимо как-то проверить, что это изначальный вызов, и вызывать  castThriftMessage только в этом случае.
			 * Также нужно добавить castThriftMessage в метод
			 * 
			 *  public byte[] process(byte[] inputData, Message inMessage, String outputChannel)
			 */
			final RpcClustered ann = controller.getClass().getAnnotation(RpcClustered.class);
			if (ann !=null){				
				castThriftMessage(ann, controllerInfo, msg, args);								
			}			

			Object ret = null;
			try{
				
				ret = controller.handle(args);				
				controller.serializeAnswer(ret, out);
				return ret;
			}catch (Exception e){
				if (e instanceof AsyncAnswer){
					log.error("Processor interface not support AsyncAnswer, controllerCls");					
				}else{
					log.error("Exception while handle thrift request in controller " + controller.getClass().getCanonicalName(), e);
				}
				controller.serializeAnswer(new TApplicationException(TApplicationException.INTERNAL_ERROR), out);
				return e;
			}finally{			
				controller.setEndNanos(System.nanoTime());
				controller.setResultSentFlag();
				logEnd(_log, controller, msg.name, null, ret);
			}			
									
		}catch (RuntimeException e){
			log.error("RuntimeException while serving thrift request", e);
			throw e;
		}
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
	
	public static void logEnd(Logger l, ThriftController c, String method, String correlationId, Object ret){
		
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
