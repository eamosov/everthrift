package com.knockchat.appserver.controller;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;

import javax.annotation.Resource;

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
import org.apache.thrift.transport.TMemoryBuffer;
import org.jgroups.Address;
import org.jgroups.blocks.ResponseMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import com.knockchat.appserver.cluster.JgroupsMessageDispatcher;
import com.knockchat.appserver.cluster.thrift.JGroupsThrift;
import com.knockchat.appserver.model.LazyLoadManager;
import com.knockchat.utils.thrift.ThriftClient;
import com.knockchat.utils.thrift.ThriftInvocationHandler.InvocationInfo;

/**
 * На каждый registry по экземпляру ThriftProcessor
 * @author fluder
 *
 */
public class ThriftProcessor implements TProcessor{
	
	private final static Logger log = LoggerFactory.getLogger(ThriftProcessor.class);
	
	private final ThriftControllerRegistry registry;
	
	@Autowired
	private JGroupsThrift jGroupsThrift;
	
	@Resource
	private  ThreadPoolTaskExecutor myExecutor;
	
	@Autowired
	private JgroupsMessageDispatcher jgroupsMessageDispatcher;
	
	@Autowired
	protected ApplicationContext applicationContext;
	
	private final TProtocolFactory protocolFactory;
	
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
				
	public MessageWrapper process(MessageWrapper in, ThriftClient thriftClient) throws Exception{

		try{		
			final TProtocol inp = protocolFactory.getProtocol(in.getTTransport());
			final TMessage msg = inp.readMessageBegin();
		
			final LogEntry logEntry = new LogEntry(msg.name);		
			logEntry.seqId = msg.seqid;
						
			final ThriftControllerInfo controllerInfo = registry.getController(msg.name);
			
			if (controllerInfo == null){
				TProtocolUtil.skip( inp, TType.STRUCT );
				inp.readMessageEnd();
				

				log.debug("No controllerCls for method {}", msg.name);
				
				final TMemoryBuffer outT = new TMemoryBuffer(1024);									
				final TProtocol out = protocolFactory.getProtocol(outT);
				final TApplicationException x = new TApplicationException( TApplicationException.UNKNOWN_METHOD, "No controllerCls for method " + msg.name);
				out.writeMessageBegin( new TMessage( msg.name, TMessageType.EXCEPTION, msg.seqid));
				x.write(out);
				out.writeMessageEnd();
				out.getTransport().flush(msg.seqid);											
				return new MessageWrapper(outT);
			}
			
			final TBase args = controllerInfo.makeArgument();
			args.read(inp);
			inp.readMessageEnd();
			
			final Logger log = LoggerFactory.getLogger(controllerInfo.controllerCls);
			
			logStart(log, msg.name, in.getSessionId(), args);
						
			final ThriftController controller = controllerInfo.makeController(args, new MessageWrapper(null).copyAttributes(in), logEntry, msg.seqid, thriftClient, registry.getType(), this.protocolFactory);
			LazyLoadManager.enable();
			final Object ret = controller.handle(args);			
			final TMemoryBuffer outT = controller.serializeAnswer(ret);
						
			controller.setEndNanos(System.nanoTime());
			controller.setResultSentFlag();
			logEnd(log, controller, msg.name, in.getSessionId(), ret);
			return new MessageWrapper(outT).copyAttributes(in);				
		}catch (AsyncAnswer e){
			return null;
		}		
	}
	
	private Future<Map<Address, Object>> castThriftMessage(final RpcClustered ann, final ThriftControllerInfo ctrl, final TMessage msg, final TBase args){
		final Callable<Map<Address, Object>> task = new Callable<Map<Address, Object>>(){

			@Override
			public Map<Address, Object> call() {
				try {
					final Set<Address> me = Collections.singleton(jgroupsMessageDispatcher.getLocalAddress());
					final InvocationInfo ii = new InvocationInfo(msg.name, args, ctrl.resultCls.getConstructor());
					final Map<Address, Object> clusterResults = jGroupsThrift.thriftCall(null, me, ann.timeout(), msg.seqid, ann.value(), ii);
					log.debug("Cluster results:{}", clusterResults);
					return clusterResults;
				} catch (TException | NoSuchMethodException | SecurityException e) {
					throw new RuntimeException(e);
				}
			}};
			
		if (ann.value() == ResponseMode.GET_NONE){
			return myExecutor.submit(task);
		}else{
			final FutureTask<Map<Address, Object>> ret = new FutureTask<Map<Address, Object>>(task);
			ret.run();
			return ret;
		}		
	}

	@Override
	public boolean process(TProtocol inp, TProtocol out) throws TException {
		return process(inp, out, null);
	}
	
	public boolean process(TProtocol inp, TProtocol out, MessageWrapper attributes) throws TException {
		final TMessage msg = inp.readMessageBegin();

		final ThriftControllerInfo controllerInfo = registry.getController(msg.name);

		try{
			
			if (controllerInfo == null){
				TProtocolUtil.skip( inp, TType.STRUCT );
				inp.readMessageEnd();
				
				log.debug("No controllerCls for method {}", msg.name);
								
				final TApplicationException x = new TApplicationException( TApplicationException.UNKNOWN_METHOD, "No controllerCls for method " + msg.name);
				out.writeMessageBegin( new TMessage( msg.name, TMessageType.EXCEPTION, msg.seqid));
				x.write(out);
				out.writeMessageEnd();
				out.getTransport().flush(msg.seqid);							
				return true;
			}
			
			final TBase args = controllerInfo.makeArgument();
			args.read(inp);
			inp.readMessageEnd();
			
			final Logger _log = LoggerFactory.getLogger(controllerInfo.controllerCls);
			
			logStart(_log, msg.name, null, args);
			
			final LogEntry logEntry = new LogEntry(msg.name);
			
			final ThriftController controller = controllerInfo.makeController(args, attributes, logEntry, msg.seqid, null, registry.getType(), this.protocolFactory);			
			
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
				
				LazyLoadManager.enable();
				ret = controller.handle(args);				
				controller.serializeAnswer(ret, out);
				
			}finally{
				controller.setEndNanos(System.nanoTime());
				controller.setResultSentFlag();
				logEnd(_log, controller, msg.name, null, ret);
			}			
									
		}catch (AsyncAnswer e){
			log.error("Processor interface not support AsyncAnswer, controllerCls");
			throw new RuntimeException("Processor interface not support AsyncAnswer", e);
		}catch (Exception e){
			log.error("Exception while serving thrift request", e);
		}
		
		return true;
	}
	
	private void logStart(Logger l, String method, String correlationId, Object args){		
		if(l.isDebugEnabled()){
			final String data = args == null ? null : args.toString();
			l.debug("START: method:{} args:{} correlationId:{}", method, data !=null ? ((data.length() > 200 && !log.isTraceEnabled()) ? data.substring(0, 199) + "..." : data) : null, correlationId);
		}
	}
	
	public static void logEnd(Logger l, ThriftController c, String method, String correlationId, Object ret){
		if (l.isDebugEnabled()){
			final String data = ret == null ? null : ret.toString();  
			l.debug("END: method:{} ctrl:{} delay:{} mcs correlationId: {} return: {}", method, c.ctrlLog(), c.getExecutionMcs(), correlationId, data !=null ? ((data.length() > 200 && !log.isTraceEnabled()) ? data.substring(0, 199) + "..." : data) : null);
		}		
	}
	
}
