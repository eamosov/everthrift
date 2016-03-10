package com.knockchat.appserver.transport.tcp;

import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.knockchat.appserver.controller.ThriftProcessor;

public class ThriftServer{
	
	private static final Logger log = LoggerFactory.getLogger(ThriftServer.class);
	
	@Value("${thrift.port}")
	private int port;
	
	@Value("${thrift.host}")
	private String host;
	
	private final RpcSyncTcpRegistry registry;
	private final TProtocolFactory protocolFactory;
	private final ApplicationContext context;
		
	private TServer server;
	private TServerSocket trans;
	
	private Thread thread;
	
	public ThriftServer(ApplicationContext context, TProtocolFactory protocolFactory, RpcSyncTcpRegistry registry){
		this.protocolFactory = protocolFactory;
		this.registry = registry;
		this.context = context;
	}
	
	@PostConstruct
	public synchronized void start(){
	
		thread = new Thread(() -> {
			threadRun();
		});
		
		thread.setName("ThriftServer");
		thread.start();
	}
	
	@PreDestroy
	public synchronized void stop() throws InterruptedException{
		if (thread !=null){
			thread.interrupt();
			thread.join();
			thread = null;
		}
	}
	
	private void threadRun() {
		
		log.info("Starting ThriftServer on {}:{}", host, port);
		
		try {
			trans = new TServerSocket(new InetSocketAddress(host, port));
		} catch (TTransportException e) {
			throw new RuntimeException(e);
		}
		
		final SynchronousQueue<Runnable> executorQueue = new SynchronousQueue<Runnable>();
		final ExecutorService es = new ThreadPoolExecutor(5, Integer.MAX_VALUE, 60, TimeUnit.SECONDS, executorQueue, new ThreadFactoryBuilder().setDaemon(false).setPriority(Thread.NORM_PRIORITY).setNameFormat("thrift-%d").build());
			
        final TThreadPoolServer.Args args = new TThreadPoolServer.Args(trans).executorService(es);
        args.transportFactory(new TFramedTransport.Factory());
        args.protocolFactory(protocolFactory);
        final ThriftProcessor tp = ThriftProcessor.create(context, registry, protocolFactory);
        args.processor(tp);
        server = new TThreadPoolServer(args);
        server.serve();
	}
	
	public void setPort(int port){
		this.port = port;
	}
	
	public int getPort() {
		return port;
	}

	public void setHost(String host){
		this.host = host;
	}
	
	public String getHost() {
		return host;
	}	
}
