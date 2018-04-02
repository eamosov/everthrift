package org.everthrift.jms;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.everthrift.appserver.controller.ThriftControllerInfo;
import org.everthrift.appserver.controller.ThriftProcessor;
import org.everthrift.clustering.jms.JmsThriftClientIF;
import org.everthrift.clustering.jms.JmsThriftClientImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationContext;
import org.springframework.context.SmartLifecycle;
import org.springframework.jms.listener.AbstractMessageListenerContainer;
import org.springframework.jms.listener.DefaultMessageListenerContainer;
import org.springframework.jms.listener.SessionAwareMessageListener;
import org.springframework.jmx.export.MBeanExporter;

import javax.jms.BytesMessage;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Session;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import java.util.Hashtable;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class JmsThriftClientServerImpl implements SmartLifecycle, JmsThriftClientIF {

    private final static Logger log = LoggerFactory.getLogger(JmsThriftClientServerImpl.class);

    private final ConnectionFactory jmsConnectionFactory;

    @Autowired
    private ApplicationContext context;

    @Autowired
    @Qualifier("mbeanExporter")
    private MBeanExporter exporter;

    private final JmsThriftClientImpl jmsThriftClient;

    private final ThriftProcessor thriftProcessor;

    private final RpcJmsRegistry rpcJmsRegistry;

    private String queuePrefix = "";

    private String queueSuffix = "";

    private final TProtocolFactory binaryProtocolFactory = new TBinaryProtocol.Factory();

    private List<AbstractMessageListenerContainer> listeners = Lists.newArrayList();

    private boolean running = false;

    private SessionAwareMessageListener<Message> listener = new SessionAwareMessageListener<Message>() {

        @Override
        public void onMessage(final Message message, Session session) throws JMSException {

            log.debug("onMessage:{}", message);

            if (!(message instanceof BytesMessage)) {
                throw new JMSException("invalid message class: " + message.getClass().getSimpleName());
            }

            try {
                final Object ret = thriftProcessor.process(binaryProtocolFactory.getProtocol(new TTransport() {

                                                               @Override
                                                               public boolean isOpen() {
                                                                   return true;
                                                               }

                                                               @Override
                                                               public void open() throws TTransportException {
                                                               }

                                                               @Override
                                                               public void close() {
                                                               }

                                                               @Override
                                                               public int read(byte[] buf, int off, int len) throws TTransportException {
                                                                   try {
                                                                       if (off == 0) {
                                                                           return ((BytesMessage) message).readBytes(buf, len);
                                                                       } else {
                                                                           final byte b[] = new byte[len];
                                                                           final int r = ((BytesMessage) message).readBytes(b, len);
                                                                           if (r > 0) {
                                                                               System.arraycopy(b, 0, buf, off, r);
                                                                           }
                                                                           return r;
                                                                       }
                                                                   } catch (JMSException e) {
                                                                       throw new TTransportException(e);
                                                                   }
                                                               }

                                                               @Override
                                                               public void write(byte[] buf, int off, int len) throws TTransportException {
                                                                   throw new TTransportException("not implemented");
                                                               }
                                                           }),

                                                           binaryProtocolFactory.getProtocol(new TTransport() {

                                                               @Override
                                                               public boolean isOpen() {
                                                                   return true;
                                                               }

                                                               @Override
                                                               public void open() throws TTransportException {
                                                               }

                                                               @Override
                                                               public void close() {
                                                               }

                                                               @Override
                                                               public int read(byte[] buf, int off, int len) throws TTransportException {
                                                                   throw new TTransportException("not implemented");
                                                               }

                                                               @Override
                                                               public void write(byte[] buf, int off, int len) throws TTransportException {
                                                               }
                                                           }), null);

                if (ret instanceof TApplicationException) {
                    throw asJMSException((TApplicationException) ret);
                } else if (ret instanceof TException) {
                    return;
                } else if (ret instanceof Exception) {
                    throw asJMSException((Exception) ret);
                }
            } catch (TException e) {
                throw asJMSException(e);
            }
        }
    };

    public JmsThriftClientServerImpl(ConnectionFactory jmsConnectionFactory,
                                     RpcJmsRegistry rpcJmsRegistry,
                                     ThriftProcessor thriftProcessor) {
        this.rpcJmsRegistry = rpcJmsRegistry;
        this.thriftProcessor = thriftProcessor;
        this.jmsConnectionFactory = jmsConnectionFactory;
        this.jmsThriftClient = new JmsThriftClientImpl(jmsConnectionFactory);
        this.jmsThriftClient.setQueuePrefix(queuePrefix);
        this.jmsThriftClient.setQueueSuffix(queueSuffix);
    }

    private JMSException asJMSException(Exception e) {

        if (e instanceof JMSException) {
            return (JMSException) e;
        }

        if (e.getCause() != null && e.getCause() instanceof JMSException) {
            return (JMSException) e.getCause();
        }

        if (e.getSuppressed() != null) {
            for (Throwable s : e.getSuppressed()) {
                if (s != null && s instanceof JMSException) {
                    return (JMSException) s;
                }
            }
        }

        final JMSException je = new JMSException(e.getMessage());
        je.setLinkedException(e);
        return je;
    }

    private synchronized DefaultMessageListenerContainer addJmsListener(String queueName, String serviceName) {

        final int consumers = Integer.parseInt(context.getEnvironment()
                                                      .getProperty("jms." + serviceName + ".consumers", "1"));

        final DefaultMessageListenerContainer l = (DefaultMessageListenerContainer) context.getBean("thriftJmsMessageListener",
                                                                                                    queueName,
                                                                                                    jmsConnectionFactory,
                                                                                                    listener,
                                                                                                    consumers);

        try {
            final Hashtable h = new Hashtable();
            h.put("name", "JmsContainerManager");
            h.put("service", serviceName);
            h.put("queue", queueName);
            exporter.registerManagedResource(context.getBean("jmsContainerManager", l, queueName), new ObjectName("JMS", h));
        } catch (MalformedObjectNameException e) {
            throw Throwables.propagate(e);
        }


        listeners.add(l);
        return l;
    }

    @Override
    public <T> T onIface(Class<T> cls) {
        return jmsThriftClient.onIface(cls);
    }

    public String getQueuePrefix() {
        return queuePrefix;
    }

    public void setQueuePrefix(String queuePrefix) {
        this.queuePrefix = queuePrefix;
        this.jmsThriftClient.setQueuePrefix(queuePrefix);
    }

    public String getQueueSuffix() {
        return queueSuffix;
    }

    public void setQueueSuffix(String queueSuffix) {
        this.queueSuffix = queueSuffix;
        this.jmsThriftClient.setQueueSuffix(queueSuffix);
    }

    public String getQueueName(String serviceName) {
        return queuePrefix + serviceName + queueSuffix;
    }


    @Override
    public boolean isAutoStartup() {
        return true;
    }

    @Override
    public void stop(Runnable callback) {
        stop();
        callback.run();
    }

    @Override
    public void start() {
        running = true;

        final Set<String> services = Sets.newHashSet();
        services.addAll(rpcJmsRegistry.getControllers()
                                      .values()
                                      .stream()
                                      .map(ThriftControllerInfo::getServiceName)
                                      .collect(Collectors.toList()));

        for (String s : services) {
            addJmsListener(getQueueName(s), s);
        }
    }

    @Override
    public void stop() {
        running = false;
        for (AbstractMessageListenerContainer l : listeners) {
            l.stop();
            l.destroy();
        }
        listeners.clear();
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public int getPhase() {
        return Integer.MAX_VALUE;
    }
}
