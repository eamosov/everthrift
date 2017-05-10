package org.everthrift.jms;

import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TMemoryBuffer;
import org.everthrift.appserver.controller.ThriftProcessor;
import org.everthrift.clustering.jms.JmsThriftClientIF;
import org.everthrift.clustering.thrift.NullResult;
import org.everthrift.clustering.thrift.ServiceIfaceProxy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.lang.reflect.Proxy;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class LocalJmsThriftClientServerImpl implements JmsThriftClientIF {

    private static final Logger log = LoggerFactory.getLogger(LocalJmsThriftClientServerImpl.class);

    @Autowired
    private ApplicationContext applicationContext;

    @Autowired
    private RpcJmsRegistry rpcJmsRegistry;

    private TProcessor thriftProcessor;

    private final TProtocolFactory binary = new TBinaryProtocol.Factory();

    private final ExecutorService executor;

    private final boolean block;

    public LocalJmsThriftClientServerImpl(boolean block) {

        this.block = block;

        final ThreadFactory tf = new ThreadFactory() {

            private final AtomicInteger threadNumber = new AtomicInteger(1);

            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r, "LocalJmsThriftTransport-" + threadNumber.getAndIncrement());
                if (t.isDaemon()) {
                    t.setDaemon(false);
                }
                if (t.getPriority() != Thread.NORM_PRIORITY) {
                    t.setPriority(Thread.NORM_PRIORITY);
                }
                return t;
            }
        };

        if (block) {
            executor = new ThreadPoolExecutor(1, Integer.MAX_VALUE, 10L, TimeUnit.SECONDS, new SynchronousQueue<Runnable>(), tf);
        } else {
            executor = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(), tf);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T onIface(Class<T> cls) {
        return (T) Proxy.newProxyInstance(LocalJmsThriftClientServerImpl.class.getClassLoader(), new Class[]{cls},
                                          new ServiceIfaceProxy(cls, ii -> {

                                              final TMemoryBuffer in = ii.buildCall(0, binary);
                                              final TProtocol inP = binary.getProtocol(in);
                                              final TMemoryBuffer out = new TMemoryBuffer(1024);
                                              final TProtocol outP = binary.getProtocol(out);

                                              final Future f = executor.submit(() -> {
                                                  try {
                                                      thriftProcessor.process(inP, outP);
                                                  } catch (Exception e) {
                                                      log.error("Exception", e);
                                                  }
                                              });

                                              if (block) {
                                                  try {
                                                      f.get();
                                                  } catch (InterruptedException | ExecutionException e) {
                                                      log.error("Exception", e);
                                                  }
                                              }

                                              throw new NullResult();
                                          }));
    }

    @PostConstruct
    private void postConstruct() {
        thriftProcessor = ThriftProcessor.create(applicationContext, rpcJmsRegistry);
    }

    @PreDestroy
    private void onDestroy() {
        executor.shutdown();
    }

    public TProcessor getThriftProcessor() {
        return thriftProcessor;
    }

    public void setThriftProcessor(TProcessor thriftProcessor) {
        this.thriftProcessor = thriftProcessor;
    }

    public boolean isBlock() {
        return block;
    }
}
