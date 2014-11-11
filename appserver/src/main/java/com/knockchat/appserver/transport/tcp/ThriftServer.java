package com.knockchat.appserver.transport.tcp;

import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.annotation.Autowired;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.knockchat.appserver.controller.ThriftProcessor;
import com.knockchat.appserver.controller.ThriftProcessorFactory;
import com.knockchat.utils.NetUtils;

public class ThriftServer extends Thread implements DisposableBean{
	
	private static final Logger log = LoggerFactory.getLogger(ThriftServer.class);
	
	private int port;
	private String host = "0.0.0.0";
	
	@Autowired
	private RpcSyncTcpRegistry registry;
	
	@Autowired
	private ThriftProcessorFactory tpf; 	
	
	private TServer server;
	private TServerSocket trans;
	
	private final TProtocolFactory protocolFactory;
	
	public ThriftServer(TProtocolFactory protocolFactory){
		this.protocolFactory = protocolFactory;
	}
	
	public void setPort(int port){
		this.port = port;
	}
	
	public void setHost(String host){
		this.host = host;
	}

	@Override
	public void run() {
		
		log.info("Starting ThriftServer on port {}", port);
		
		try {
			trans = new TServerSocket(new InetSocketAddress(host, port));
		} catch (TTransportException e) {
			throw new RuntimeException(e);
		}
		
		if (trans.getServerSocket().getInetAddress().isAnyLocalAddress()){
			this.host = NetUtils.getFirstPublicHostAddress();
		}
		
		final SynchronousQueue<Runnable> executorQueue = new SynchronousQueue<Runnable>();
		final ExecutorService es = new ThreadPoolExecutor(5, Integer.MAX_VALUE, 60, TimeUnit.SECONDS, executorQueue, new ThreadFactoryBuilder().setDaemon(false).setPriority(Thread.NORM_PRIORITY).setNameFormat("thrift-%d").build());
			
        final TThreadPoolServer.Args args = new TThreadPoolServer.Args(trans).executorService(es);
        args.transportFactory(new TFramedTransport.Factory());
        args.protocolFactory(protocolFactory);
        final ThriftProcessor tp = tpf.getThriftProcessor(registry, protocolFactory);
        args.processor(tp);
        server = new TThreadPoolServer(args);
        server.serve();
	}
	
	@Override
	public void destroy(){
		server.stop();
	}

	public int getPort() {
		return port;
	}

	public String getHost() {
		return host;
	}
	
	public String getLocalAddress(){
		return getHost();
//		final InetAddress inet =  trans.getServerSocket().getInetAddress();
//		return inet == null ? null : inet.getHostAddress();
	}
}
