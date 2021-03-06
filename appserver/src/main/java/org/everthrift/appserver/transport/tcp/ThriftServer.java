package org.everthrift.appserver.transport.tcp;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportException;
import org.everthrift.appserver.controller.ThriftProcessor;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.context.SmartLifecycle;

import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ThriftServer implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(ThriftServer.class);

    @Value("${thrift.port}")
    private int port;

    @Value("${thrift.host}")
    private String host;

    private final TProtocolFactory protocolFactory;

    private TServer server;

    private TServerSocket trans;

    private ThriftProcessor thriftProcessor;

    @Nullable
    private Thread thread;

    public ThriftServer(TProtocolFactory protocolFactory, ThriftProcessor thriftProcessor) {
        this.protocolFactory = protocolFactory;
        this.thriftProcessor = thriftProcessor;
    }

    @Override
    public synchronized void start() {

        thread = new Thread(() -> {
            threadRun();
        });

        thread.setName("ThriftServer");
        thread.start();
    }

    @Override
    public synchronized void stop() {
        if (thread != null) {
            server.stop();
            thread.interrupt();
            try {
                thread.join();
            } catch (InterruptedException e) {
            }
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
        final ExecutorService es = new ThreadPoolExecutor(5, Integer.MAX_VALUE, 60, TimeUnit.SECONDS, executorQueue,
                                                          new ThreadFactoryBuilder().setDaemon(false)
                                                                                    .setPriority(Thread.NORM_PRIORITY)
                                                                                    .setNameFormat("thrift-%d")
                                                                                    .build());

        final TThreadPoolServer.Args args = new TThreadPoolServer.Args(trans).executorService(es);
        args.transportFactory(new TFramedTransport.Factory());
        args.protocolFactory(protocolFactory);
        args.processor(thriftProcessor);
        server = new TThreadPoolServer(args);
        server.serve();
    }

    public void setPort(int port) {
        this.port = port;
    }

    public int getPort() {
        return port;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getHost() {
        return host;
    }

    @Override
    public boolean isRunning() {
        return thread != null;
    }

    @Override
    public int getPhase() {
        return 100;
    }

    @Override
    public boolean isAutoStartup() {
        return true;
    }

    @Override
    public void stop(@NotNull Runnable callback) {
        stop();
        callback.run();
    }
}
