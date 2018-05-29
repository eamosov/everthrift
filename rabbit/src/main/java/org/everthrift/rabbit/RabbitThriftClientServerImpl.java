package org.everthrift.rabbit;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.rabbitmq.client.AMQP.Exchange.DeclareOk;
import com.rabbitmq.client.Channel;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TMemoryInputTransport;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.everthrift.clustering.thrift.ThriftControllerDiscovery;
import org.everthrift.appserver.controller.ThriftControllerInfo;
import org.everthrift.appserver.controller.ThriftProcessor;
import org.everthrift.clustering.rabbit.RabbitThriftClientIF;
import org.everthrift.clustering.rabbit.RabbitThriftClientImpl;
import org.everthrift.thrift.ThriftServicesDiscovery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.FanoutExchange;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageListener;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.ChannelCallback;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.List;
import java.util.stream.Collectors;

public class RabbitThriftClientServerImpl implements RabbitThriftClientIF {

    private static final Logger log = LoggerFactory.getLogger(RabbitThriftClientServerImpl.class);

    @Autowired
    private ConnectionFactory cf;

    @Autowired
    private ApplicationContext context;

    @Autowired
    private ThriftServicesDiscovery thriftServicesDb;

    @Autowired
    private ThriftControllerDiscovery thriftControllerDiscovery;

    private List<SimpleMessageListenerContainer> listeners = Lists.newArrayList();

    private final TProtocolFactory binaryProtocolFactory = new TBinaryProtocol.Factory();

    private final ThriftProcessor thriftProcessor;

    private final RabbitThriftClientImpl rabbitThriftClient;

    private final RabbitAdmin admin;

    private String queuePrefix = "";

    private String queueSuffix = "";

    private MessageListener listener = new MessageListener() {

        @Override
        public void onMessage(Message message) {

            log.debug("onMessage:{}", message);

            final TTransport t = new TMemoryInputTransport(message.getBody());

            try {
                final Object ret = thriftProcessor.process(binaryProtocolFactory.getProtocol(t),
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
                    throw new RuntimeException((TApplicationException) ret);
                } else if (ret instanceof TException) {
                    return;
                } else if (ret instanceof Exception) {
                    throw Throwables.propagate((Exception) ret);
                }
            } catch (TException e) {
                throw new RuntimeException((TException) e);
            }
        }

    };

    public RabbitThriftClientServerImpl(ConnectionFactory connectionFactory,
                                        ThriftProcessor thriftProcessor) {
        this.cf = connectionFactory;
        this.rabbitThriftClient = new RabbitThriftClientImpl(cf, thriftServicesDb);
        this.admin = new RabbitAdmin(cf);
        this.thriftProcessor = thriftProcessor;
        // this.admin.setIgnoreDeclarationExceptions(true);

    }

    @PostConstruct
    public void attachListeners() throws Exception {

        thriftControllerDiscovery.getLocal(thriftProcessor.registryAnn.getSimpleName())
                                 .stream()
                                 .map(ThriftControllerInfo::getServiceName)
                                 .collect(Collectors.toSet()).forEach(this::addListener);
    }

    @PreDestroy
    public synchronized void destroy() throws Exception {

        for (SimpleMessageListenerContainer l : listeners) {
            l.stop();
            l.destroy();
        }
        listeners.clear();
    }

    private synchronized SimpleMessageListenerContainer addListener(final String serviceName) {

        log.debug("addListener ({})", serviceName);

        final String exchangeName = rabbitThriftClient.getExchangeName(serviceName);
        final String queueName = getQueueName(serviceName);

        final Queue queue = new Queue(queueName);
        admin.declareQueue(queue);

        if (!isExchangeExists(exchangeName)) {
            log.info("Creating exchange '{}'", exchangeName);
            final FanoutExchange exchange = new FanoutExchange(exchangeName);
            admin.declareExchange(exchange);

            admin.declareBinding(BindingBuilder.bind(queue).to(exchange));
        } else {
            log.debug("Exchange '{}' has been allready existed");
        }

        final int consumers = Integer.parseInt(context.getEnvironment()
                                                      .getProperty("rabbit." + serviceName + ".consumers", "1"));


        final SimpleMessageListenerContainer l = (SimpleMessageListenerContainer) context.getBean("thriftRabbitMessageListener",
                                                                                                  queue.getName(), cf, listener, consumers);
        listeners.add(l);
        return l;
    }

    public boolean isExchangeExists(final String exchange) {
        return admin.getRabbitTemplate().execute(new ChannelCallback<DeclareOk>() {
            @Override
            public DeclareOk doInRabbit(Channel channel) throws Exception {
                try {
                    return channel.exchangeDeclarePassive(exchange);
                } catch (Exception e) {
                    return null;
                }
            }
        }) != null;
    }

    @Override
    public <T> T onIface(Class<T> cls) {
        return rabbitThriftClient.onIface(cls);
    }

    public String getQueuePrefix() {
        return queuePrefix;
    }

    public void setQueuePrefix(String queuePrefix) {
        this.queuePrefix = queuePrefix;
    }

    public String getQueueSuffix() {
        return queueSuffix;
    }

    public void setQueueSuffix(String queueSuffix) {
        this.queueSuffix = queueSuffix;
    }

    public String getQueueName(String serviceName) {
        return queuePrefix + serviceName + queueSuffix;
    }

    public String getExchangePrefix() {
        return rabbitThriftClient.getExchangePrefix();
    }

    public void setExchangePrefix(String exchangePrefix) {
        rabbitThriftClient.setExchangePrefix(exchangePrefix);
    }

    public String getExchangeSuffix() {
        return rabbitThriftClient.getExchangeSuffix();
    }

    public void setExchangeSuffix(String exchangeSuffix) {
        rabbitThriftClient.setExchangeSuffix(exchangeSuffix);
    }

}
