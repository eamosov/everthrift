package org.everthrift.rabbit;

import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TMemoryBuffer;
import org.everthrift.appserver.controller.ThriftProcessor;
import org.everthrift.clustering.rabbit.RabbitThriftClientIF;
import org.everthrift.clustering.thrift.NullResult;
import org.everthrift.clustering.thrift.ServiceIfaceProxy;
import org.everthrift.utils.ThriftServicesDb;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

public class LocalRabbitThriftClientServerImpl implements RabbitThriftClientIF {

    private static final Logger log = LoggerFactory.getLogger(LocalRabbitThriftClientServerImpl.class);

    private final TProcessor thriftProcessor;

    private final TProtocolFactory binary = new TBinaryProtocol.Factory();

    private final ExecutorService executor;

    private final ThriftServicesDb thriftServicesDb;

    private boolean block = false;

    public LocalRabbitThriftClientServerImpl(boolean block, ThriftProcessor thriftProcessor, ThriftServicesDb thriftServicesDb) {

        this.block = block;
        this.thriftProcessor = thriftProcessor;
        this.thriftServicesDb = thriftServicesDb;

        final ThreadFactory tf = new ThreadFactory() {

            private final AtomicInteger threadNumber = new AtomicInteger(1);

            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r, "LocalRabbitThriftTransport-" + threadNumber.getAndIncrement());
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
        return (T) Proxy.newProxyInstance(LocalRabbitThriftClientServerImpl.class.getClassLoader(), new Class[]{cls},
                                          new ServiceIfaceProxy(thriftServicesDb, ii -> {

                                              final TMemoryBuffer in = ii.serializeCall(0, binary);
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

    @PreDestroy
    private void onDestroy() {
        executor.shutdown();
    }

    public TProcessor getThriftProcessor() {
        return thriftProcessor;
    }

    public boolean isBlock() {
        return block;
    }
}
