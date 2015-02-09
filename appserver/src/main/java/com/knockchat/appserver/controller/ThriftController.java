package com.knockchat.appserver.controller;

import java.lang.annotation.Annotation;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import javax.sql.DataSource;

import org.apache.thrift.TApplicationException;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TMessage;
import org.apache.thrift.protocol.TMessageType;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TMemoryBuffer;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.integration.MessageChannel;
import org.springframework.integration.MessageHeaders;
import org.springframework.integration.message.GenericMessage;
import org.springframework.transaction.TransactionStatus;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.knockchat.appserver.AppserverApplication;
import com.knockchat.appserver.model.LazyLoadManager;
import com.knockchat.appserver.model.LazyLoadManager.LoadList;
import com.knockchat.utils.ExecutionStats;
import com.knockchat.utils.Pair;
import com.knockchat.utils.thrift.ThriftClient;

public abstract class ThriftController<ArgsType extends TBase, ResultType> {

	protected final Logger log = LoggerFactory.getLogger(this.getClass());
	
	protected ArgsType args;
	protected ThriftControllerInfo info;
	protected LogEntry logEntry;
	protected int seqId;
	protected AbstractApplicationContext context;
	protected DataSource ds;
	protected TransactionStatus transactionStatus;
	protected ThriftClient thriftClient;
	
	@Autowired
	private ApplicationContext applicationContext;

	protected MessageHeaders inHeaders;
	protected MessageChannel outputChannel;
	private long startNanos;
	private long endNanos;
	
	protected boolean noProfile=false;
	
	protected Class<? extends Annotation> registryAnn;
	protected TProtocolFactory protocolFactory;
	
	/**
	 * Флаг, показывающий был лио отправлен какой-либо ответ (результат или
	 * исключение)
	 */
	private boolean resultSent = false;
	
	public static final ConcurrentHashMap<String, ExecutionStats> rpcControllesStats = new ConcurrentHashMap<String, ExecutionStats>();
		
	public abstract void setup(ArgsType args);
	
	public String ctrlLog(){
		return "";
	}
		
	public void setup (ArgsType args, ThriftControllerInfo info, LogEntry logEntry, int seqId, MessageHeaders inHeaders,MessageChannel outputChannel, ThriftClient thriftClient, Class<? extends Annotation> registryAnn, TProtocolFactory protocolFactory){
		this.args = args;
		this.info = info;
		this.logEntry = logEntry;
		this.seqId = seqId;
		this.context = AppserverApplication.INSTANCE.context;
		this.inHeaders = inHeaders;
		this.outputChannel = outputChannel;
		this.thriftClient = thriftClient;
		this.registryAnn = registryAnn;
		this.protocolFactory = protocolFactory;
		
		this.startNanos = System.nanoTime();
		
		try{
			this.ds = context.getBean(DataSource.class);
		}catch (NoSuchBeanDefinitionException e){
			this.ds = null;
		}
		
		rpcControllesStats.putIfAbsent(this.getClass().getSimpleName(), new ExecutionStats());
	}
	
	protected abstract ResultType handle() throws TException;
	
	
	/**
	 * 
	 * @param args
	 * @return TApplicationException || TException || ResultType
	 */
	protected Object handle(ArgsType args){
		try {
			setup(args);
			final ResultType result =  handle();			
			return this.filterOutput(loadLazyRelations(result));
		} catch (TException e) {
			return e;
		}		
	}
	
	protected void setTransactionStatus(TransactionStatus transactionStatus){
		this.transactionStatus = transactionStatus;
	}
	
	protected ResultType waitForAnswer(){
		throw new AsyncAnswer();		
	}
	
	protected void waitForAnswerSuccess(ResultType result){
		
	}
	
	protected ResultType waitForAnswer(ListenableFuture<ResultType> lf){
				
		Futures.addCallback(lf, new FutureCallback<ResultType>(){

			@Override
			public void onSuccess(ResultType result) {
				waitForAnswerSuccess(result);
				ThriftController.this.sendAnswer(result);				
			}

			@Override
			public void onFailure(Throwable t) {
				
				if (t instanceof TException)
					sendException((TException)t);
				else
					log.error("FutureCallback.onFailure", t);
					
			}});
		
		throw new AsyncAnswer();		
	}
	
	protected synchronized boolean sendException(TException answer){
		return sendAnswerOrException(answer);
	}
	
	protected synchronized boolean sendAnswer(ResultType answer){
		return sendAnswerOrException((ResultType)answer);
	}
	
	protected synchronized boolean sendAnswerOrException(Object answer){
		
		final TMemoryBuffer payload;
		
		if (!(answer instanceof Exception))
			answer = this.filterOutput(this.loadLazyRelations((ResultType)answer));				
		
		try{
			if (outputChannel == null || inHeaders == null)
				throw new RuntimeException("Coudn't send async message: unknown channel");
					
			try {
				payload = serializeAnswer(answer);
			} catch (TException e) {
				log.error("Exception while sending answer:", e);
				return false;
			}			
		}finally{
			setEndNanos(System.nanoTime());
		}
		
		setResultSentFlag();
				
		final GenericMessage<TMemoryBuffer> s = new GenericMessage<TMemoryBuffer>(payload, inHeaders);
		outputChannel.send(s);
		
		ThriftProcessor.logEnd(LoggerFactory.getLogger(this.getClass()), this, this.getClass().getSimpleName(), (String)(inHeaders != null ? inHeaders.getCorrelationId() : null), answer);
				
		return true;
	}
	
	/**
	 * Установить флаг отправленного запроса и вычислить время выполнения
	 * запроса
	 */
	public synchronized void setResultSentFlag() {
		if ( resultSent )
			throw new IllegalStateException( this.getClass().getSimpleName() + ": Result already sent" );

		resultSent = true;

		if (!noProfile){			
			final ExecutionStats es = rpcControllesStats.get(this.getClass().getSimpleName());
			if (es!=null) es.update(getExecutionMcs());						
		}
		
	}

	public long getEndNanos() {
		return endNanos;
	}

	public synchronized void setEndNanos(long endNanos) {
		this.endNanos = endNanos;
	}

	public long getStartNanos() {
		return startNanos;
	}

	public synchronized void setStartNanos(long startNanos) {
		this.startNanos = startNanos;
	}
	
	public long getExecutionMcs(){
		return (endNanos-startNanos)/1000;
	}
		
	public static void resetExecutionLog(){
		rpcControllesStats.clear();
	}

	public static synchronized String getExecutionLog() {
		final ArrayList<Pair<String,ExecutionStats>> list = new ArrayList<Pair<String,ExecutionStats>>( rpcControllesStats.size() );
		
		final Iterator<Entry<String, ExecutionStats>> it =  rpcControllesStats.entrySet().iterator();
		
		while(it.hasNext()){
			final Entry<String, ExecutionStats> e = it.next();
			final ExecutionStats stats;
			synchronized(e.getValue()){
				stats = new ExecutionStats(e.getValue());
			}
			list.add( new Pair<String,ExecutionStats>(e.getKey() , stats));
			
		}

		Collections.sort( list, new Comparator<Pair<String,ExecutionStats>>(){

			@Override
			public int compare( Pair<String, ExecutionStats> o1, Pair<String, ExecutionStats> o2 ) {
				return Long.signum( o2.second.getSummaryTime() - o1.second.getSummaryTime() );
			}
		} );

		return ExecutionStats.getLogString(list);
	}
	
	//Обработчик после lasyload и перед отправкой данных клиенту
	protected ResultType filterOutput(ResultType result){
		return result;
	}
	
	protected boolean checkAnswerType(Object answer){
		
		if (answer == null)
			return true;
		else if (answer instanceof TApplicationException)
			return true;
		else if (answer instanceof TException)
			return true;
		else if (answer instanceof Exception)
			return false;
		else
			return true;
	}
	
	/**
	 * 
	 * @param answer TApplicationException || TException || ResultType
	 * @param o
	 * @throws TException
	 */
	protected void serializeAnswer(Object answer, TProtocol o) throws TException{
		
		if (!checkAnswerType(answer)){
			log.error("Invalid answer: {}", answer);
			throw new TApplicationException(TApplicationException.INTERNAL_ERROR, "bad answer");
		}
		
		if (answer instanceof TApplicationException){
			o.writeMessageBegin( new TMessage(info.getName(), TMessageType.EXCEPTION, seqId ) );
			((TApplicationException)answer).write(o);
		}else{				
			o.writeMessageBegin( new TMessage( info.getName(), TMessageType.REPLY, seqId ) );				
			final TBase result = info.makeResult(answer);										
			result.write(o);
		}

		o.writeMessageEnd();
		o.getTransport().flush(seqId);								
	}
	
	/**
	 * 
	 * @param answer - TApplicationException или TException или ResultType 
	 * @return
	 * @throws TTransportException
	 * @throws TException
	 */
	protected TMemoryBuffer serializeAnswer(Object answer) throws TException{
		
		
		try{			
			final TMemoryBuffer outT = new TMemoryBuffer(1024);
			final TProtocol o = protocolFactory.getProtocol(outT);
			
			serializeAnswer(answer, o);
			
			return outT;		
		}catch (Exception e){
			final TMemoryBuffer outT = new TMemoryBuffer(1024);
			final TProtocol o = protocolFactory.getProtocol(outT);
				
			log.error("Exception while serializeAnswer", e);
			
			final TApplicationException x = new TApplicationException( TApplicationException.INTERNAL_ERROR, e.getCause() !=null ?  e.getCause().getMessage() : e.getMessage());
			o.writeMessageBegin( new TMessage( info.getName(), TMessageType.EXCEPTION, seqId ) );
			x.write(o);
			o.writeMessageEnd();
			o.getTransport().flush(seqId);			
			return outT;									
		}		
	}
	
	protected ResultType loadLazyRelations(ResultType result){
		
		if (result==null)
			return null;
		
		final LoadList ll = LazyLoadManager.get();
		ll.load(result);
		ll.enable();
		
		return result;
	}

}
