package com.knockchat.jms;

import java.lang.reflect.Proxy;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TMemoryBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;

import com.knockchat.appserver.controller.ThriftProcessor;
import com.knockchat.clustering.jms.JmsThriftClientIF;
import com.knockchat.clustering.thrift.InvocationCallback;
import com.knockchat.clustering.thrift.InvocationInfo;
import com.knockchat.clustering.thrift.NullResult;
import com.knockchat.clustering.thrift.ServiceIfaceProxy;

public class LocalJmsThriftClientServerImpl implements JmsThriftClientIF {
	
	private static final Logger log = LoggerFactory.getLogger(LocalJmsThriftClientServerImpl.class);
	
	@Autowired
	private ApplicationContext applicationContext;
	
	@Autowired
	private RpcJmsRegistry rpcJmsRegistry;
	
	private TProcessor thriftProcessor;
	
	private final TProtocolFactory binary = new TBinaryProtocol.Factory();
	
	private final ExecutorService executor;
	
	public LocalJmsThriftClientServerImpl(){
		
		executor  = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(), new ThreadFactory() {
			
	        private final AtomicInteger threadNumber = new AtomicInteger(1);

	        public Thread newThread(Runnable r) {
	            Thread t = new Thread(r, "LocalQueryThriftTransport-" + threadNumber.getAndIncrement());
	            if (t.isDaemon())
	                t.setDaemon(false);
	            if (t.getPriority() != Thread.NORM_PRIORITY)
	                t.setPriority(Thread.NORM_PRIORITY);
	            return t;
	        }
	    });
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> T onIface(Class<T> cls) {
		return (T)Proxy.newProxyInstance(LocalJmsThriftClientServerImpl.class.getClassLoader(), new Class[]{cls}, new ServiceIfaceProxy(cls, new InvocationCallback(){

			@SuppressWarnings("rawtypes")
			@Override
			public Object call(InvocationInfo ii) throws NullResult, TException {
				
				final TMemoryBuffer in = ii.buildCall(0, binary);
				final TProtocol inP = binary.getProtocol(in);
				final TMemoryBuffer out = new TMemoryBuffer(1024);
				final TProtocol outP = binary.getProtocol(out);
				
				executor.execute(() -> {
					try {
						thriftProcessor.process(inP, outP);
					} catch (Exception e) {
						log.error("Exception", e);
					}					
				});
				
				throw new NullResult();				
			}}));
	}

	@PostConstruct
	private void postConstruct(){
		thriftProcessor = ThriftProcessor.create(applicationContext, rpcJmsRegistry);		
	}
	
	@PreDestroy
	private void onDestroy(){
		executor.shutdown();
	}

	public TProcessor getThriftProcessor() {
		return thriftProcessor;
	}

	public void setThriftProcessor(TProcessor thriftProcessor) {
		this.thriftProcessor = thriftProcessor;
	}
}
