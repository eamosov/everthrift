package com.knockchat.appserver.cluster;

import java.lang.reflect.Proxy;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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
import com.knockchat.appserver.transport.jms.RpcJmsRegistry;
import com.knockchat.utils.thrift.InvocationCallback;
import com.knockchat.utils.thrift.InvocationInfo;
import com.knockchat.utils.thrift.NullResult;
import com.knockchat.utils.thrift.ServiceIfaceProxy;

public class LocalQueryThriftTransport implements QueryThriftTransport {
	
	private static final Logger log = LoggerFactory.getLogger(LocalQueryThriftTransport.class);
	
	@Autowired
	private ApplicationContext applicationContext;
	
	@Autowired
	private RpcJmsRegistry rpcJmsRegistry;
	
	private TProcessor thriftProcessor;
	
	private final TProtocolFactory binary = new TBinaryProtocol.Factory();
	
	private ExecutorService executor = Executors.newSingleThreadExecutor();

	@SuppressWarnings("unchecked")
	@Override
	public <T> T onIface(Class<T> cls) {
		return (T)Proxy.newProxyInstance(LocalQueryThriftTransport.class.getClassLoader(), new Class[]{cls}, new ServiceIfaceProxy(cls, new InvocationCallback(){

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
		thriftProcessor = ThriftProcessor.create(applicationContext, rpcJmsRegistry, new TBinaryProtocol.Factory());		
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
