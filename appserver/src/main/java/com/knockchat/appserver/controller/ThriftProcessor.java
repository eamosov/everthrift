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
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.jgroups.Address;
import org.jgroups.blocks.ResponseMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.integration.Message;
import org.springframework.integration.MessageChannel;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import com.knockchat.appserver.cluster.JgroupsMessageDispatcher;
import com.knockchat.appserver.cluster.thrift.JGroupsThrift;
import com.knockchat.appserver.model.LazyLoadManager;
import com.knockchat.appserver.model.LazyLoadManager.LoadList;
import com.knockchat.utils.thrift.ThriftClient;
import com.knockchat.utils.thrift.ThriftInvocationHandler.InvocationInfo;

/**
 * На каждый registry по экземпляру ThriftProcessor
 * @author fluder
 *
 */
public class ThriftProcessor implements TProcessor{
	
	private final static Logger log = LoggerFactory.getLogger(ThriftProcessor.class);
	
	//private Message<byte[]> inMessage;
	//private String outputChannel;
		
//	private AtomicInteger flushes = new AtomicInteger(0);
//	private AtomicInteger reads = new AtomicInteger(0);
	
	//private final LinkedList<LogEntry> calls = new LinkedList<LogEntry>();
	private final ThriftControllerRegistry registry;
	
	@Autowired
	private JGroupsThrift jGroupsThrift;
	
	@Resource
	private  ThreadPoolTaskExecutor myExecutor;
	
	@Autowired
	private JgroupsMessageDispatcher jgroupsMessageDispatcher;
	
	private final TProtocolFactory protocolFactory;
	
	public ThriftProcessor(ThriftControllerRegistry registry, TProtocolFactory protocolFactory){
		this.registry = registry;
		this.protocolFactory = protocolFactory;
	}
	
	public TMemoryBuffer process(TTransport inT) throws Exception{
		return process(inT, null, null, null);
	}
	
	public TMemoryBuffer process(TTransport inT, Message inMessage, MessageChannel outputChannel, ThriftClient thriftClient) throws Exception{

		try{		
			final TProtocol inp = protocolFactory.getProtocol(inT);
			final TMessage msg = inp.readMessageBegin();
		
			final LogEntry logEntry = new LogEntry(msg.name);		
			logEntry.seqId = msg.seqid;
						
			final ThriftControllerInfo ctrl = registry.getController(msg.name);
			
			if (ctrl == null){
				TProtocolUtil.skip( inp, TType.STRUCT );
				inp.readMessageEnd();
				
				final TMemoryBuffer outT = new TMemoryBuffer(1024);					
				log.debug("No controller for method {}", msg.name);
				writeApplicationException(protocolFactory.getProtocol(outT), msg.seqid, msg.name, TApplicationException.UNKNOWN_METHOD, "No controller for method " + msg.name);					
				return outT;
			}
			
			final TBase args = ctrl.makeArgument();
			args.read(inp);
			inp.readMessageEnd();
			
			LoggerFactory.getLogger(ctrl.controller).debug("START: method:{} args:{} correlationId:{}", msg.name, args, inMessage == null ? null: inMessage.getHeaders().getCorrelationId());			
						
			final ThriftController c = ctrl.makeController(args, logEntry, msg.seqid, inMessage == null ? null : inMessage.getHeaders(), outputChannel, thriftClient, registry.getType(), this.protocolFactory);
						
			final Object ret = handle(c, args);			
			final TMemoryBuffer outT = buildAnswer(c.seqId, c.info, ret, protocolFactory);
			c.setEndNanos(System.nanoTime());
			c.setResultSentFlag();
			LoggerFactory.getLogger(ctrl.controller).debug("END: method:{} ctr:{} delay:{} mcs return: {} correlationId:{}", msg.name, c.ctrlLog(), c.getExecutionMcs(), ret==null ? null : ret.toString(), inMessage == null ? null: inMessage.getHeaders().getCorrelationId());
			return outT;				

		}catch (AsyncAnswer e){
			return null;
		}		
	}
	
	private Object handle(ThriftController c, TBase args){
		try {
			LazyLoadManager.enable();
			c.setup(args);
			return c.handle();
		} catch (TException e) {
			return e;
		}		
	}
	
	public static TMemoryBuffer buildAnswer(int seqId, final ThriftControllerInfo ctrl, Object ret, TProtocolFactory protocolFactory) throws TTransportException, TException{
		
	
		try{			
			final TMemoryBuffer outT = new TMemoryBuffer(1024);
			final TProtocol o = protocolFactory.getProtocol(outT);

			if (ret instanceof TApplicationException){
				o.writeMessageBegin( new TMessage( ctrl.getName(), TMessageType.EXCEPTION, seqId ) );
				((TApplicationException)ret).write(o);
				o.writeMessageEnd();
				o.getTransport().flush(seqId);
			}else{
				writeAnswer(o, seqId, ctrl, ret);
			}
			
			return outT;		
		}catch (Exception e){
			final TMemoryBuffer outT = new TMemoryBuffer(1024);
				
			log.error("Exception", e);
			writeApplicationException(protocolFactory.getProtocol(outT), seqId, ctrl.getName(), TApplicationException.INTERNAL_ERROR, e.getCause() !=null ?  e.getCause().getMessage() : e.getMessage());
			return outT;									
		}		
	}

	private static void writeApplicationException(TProtocol  outp, int seqId, String name, int type, String text ) throws TException, TTransportException {
		TApplicationException x = new TApplicationException( type, text );
		outp.writeMessageBegin( new TMessage( name, TMessageType.EXCEPTION, seqId ) );
		x.write( outp );
		outp.writeMessageEnd();
		outp.getTransport().flush(seqId);
		return;
	}
	
	private static void writeAnswer(TProtocol  outp, int seqId, ThriftControllerInfo info, Object o) throws TException{
		outp.writeMessageBegin( new TMessage( info.getName(), TMessageType.REPLY, seqId ) );
		
		final TBase result = info.makeResult(o);

		final LoadList ll = LazyLoadManager.get();
		ll.load(result);
		ll.enable();
		
		result.write(outp);
		outp.writeMessageEnd();
		outp.getTransport().flush(seqId);		
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
		final TMessage msg = inp.readMessageBegin();

		final ThriftControllerInfo ctrl = registry.getController(msg.name);

		try{
			
			if (ctrl == null){
				TProtocolUtil.skip( inp, TType.STRUCT );
				inp.readMessageEnd();
				
				log.debug("No controller for method {}", msg.name);
				writeApplicationException(out, msg.seqid, msg.name, TApplicationException.UNKNOWN_METHOD, "No controller for method " + msg.name);
				return true;
			}
			
			final TBase args = ctrl.makeArgument();
			args.read(inp);
			inp.readMessageEnd();
			
			LoggerFactory.getLogger(ctrl.controller).debug("START: method:{} args:{}", msg.name, args);
			
			final LogEntry logEntry = new LogEntry(msg.name);
			
			final ThriftController c = ctrl.makeController(args, logEntry, msg.seqid, null, null, null, registry.getType(), this.protocolFactory);			
			
			/*
			 * TODO т.к. текущий метов обрабатывает только @RpcSyncTcp контроллеры, то циклический вызовов не получится,
			 * однако это решение не полностью универсально.
			 * Необходимо как-то проверить, что это изначальный вызов, и вызывать  castThriftMessage только в этом случае.
			 * Также нужно добавить castThriftMessage в метод
			 * 
			 *  public byte[] process(byte[] inputData, Message inMessage, String outputChannel)
			 */
			final RpcClustered ann = c.getClass().getAnnotation(RpcClustered.class);
			if (ann !=null){				
				castThriftMessage(ann, ctrl, msg, args);								
			}			

			Object ret = null;
			try{
				try{
					ret = handle(c, args);				
				}catch (Exception e){
					writeApplicationException(out, msg.seqid, ctrl.getName(), TApplicationException.INTERNAL_ERROR, e.getCause() !=null ?  e.getCause().getMessage() : e.getMessage());
					throw e;
				}
				
				if (ret instanceof TApplicationException){
					out.writeMessageBegin( new TMessage( ctrl.getName(), TMessageType.EXCEPTION, msg.seqid ) );
					((TApplicationException)ret).write(out);
					out.writeMessageEnd();
					out.getTransport().flush(msg.seqid);
				}else{
					writeAnswer(out, msg.seqid, ctrl, ret);
				}
				
				//out.getTransport().flush();
			}finally{
				c.setEndNanos(System.nanoTime());
				c.setResultSentFlag();
				LoggerFactory.getLogger(ctrl.controller).debug("END: method:{} ctrl:{} delay:{} mcs return: {}", msg.name, c.ctrlLog(), c.getExecutionMcs(), ret !=null ? ret.toString() : null);
			}			
									
		}catch (AsyncAnswer e){
			log.error("Processor interface not support AsyncAnswer, controller");
			throw new RuntimeException("Processor interface not support AsyncAnswer", e);
		}catch (Exception e){
			log.error("Exception while serving thrift request", e);
		}
		
		return true;
	}
	
}
