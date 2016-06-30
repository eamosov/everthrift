package org.everthrift.rabbit;

import java.util.List;
import java.util.Set;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.apache.thrift.TApplicationException;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TMemoryInputTransport;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.everthrift.appserver.controller.ThriftControllerInfo;
import org.everthrift.appserver.controller.ThriftProcessor;
import org.everthrift.clustering.rabbit.RabbitThriftClientIF;
import org.everthrift.clustering.rabbit.RabbitThriftClientImpl;
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

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.rabbitmq.client.AMQP.Exchange.DeclareOk;
import com.rabbitmq.client.Channel;

public class RabbitThriftClientServerImpl implements RabbitThriftClientIF{

    private static final Logger log = LoggerFactory.getLogger(RabbitThriftClientServerImpl.class);

    @Autowired
    private ConnectionFactory cf;

    @Autowired
    private ApplicationContext context;

    @Autowired
    private RpcRabbitRegistry rpcRabbitRegistry;

    private List<SimpleMessageListenerContainer> listeners = Lists.newArrayList();
    private final TProtocolFactory binaryProtocolFactory = new TBinaryProtocol.Factory();

    private ThriftProcessor thriftProcessor;
    private final RabbitThriftClientIF rabbitThriftClient;
    private final RabbitAdmin admin;

    private MessageListener listener = new MessageListener(){

        @Override
        public void onMessage(Message message) {

            log.debug("onMessage:{}", message);

            final TTransport t =  new TMemoryInputTransport(message.getBody());

            try {
                final Object ret = thriftProcessor.process(
                        binaryProtocolFactory.getProtocol(t),
                        binaryProtocolFactory.getProtocol(
                                new TTransport(){

                                    @Override
                                    public boolean isOpen() {return true;}

                                    @Override
                                    public void open() throws TTransportException {}

                                    @Override
                                    public void close() {}

                                    @Override
                                    public int read(byte[] buf, int off, int len) throws TTransportException {
                                        throw new TTransportException("not implemented");
                                    }

                                    @Override
                                    public void write(byte[] buf, int off, int len)	throws TTransportException {
                                    }
                                }),
                        null);

                if (ret instanceof TApplicationException){
                    throw new RuntimeException((TApplicationException)ret);
                }else if (ret instanceof TException){
                    return;
                }else if (ret instanceof Exception)
                    throw Throwables.propagate((Exception)ret);
            } catch (TException e) {
                throw new RuntimeException((TException)e);
            }
        }

    };

    public RabbitThriftClientServerImpl(ConnectionFactory connectionFactory){
        this.cf = connectionFactory;
        this.rabbitThriftClient = new RabbitThriftClientImpl(cf);
        this.admin = new RabbitAdmin(cf);
        //this.admin.setIgnoreDeclarationExceptions(true);
    }


    @PostConstruct
    public void attachListeners() throws Exception {

        thriftProcessor = ThriftProcessor.create(context, rpcRabbitRegistry);

        final Set<String> services = Sets.newHashSet();
        for (ThriftControllerInfo i:rpcRabbitRegistry.getControllers().values())
            services.add(i.getServiceName());

        for(String s: services)
            addListener(s);
    }

    @PreDestroy
    public synchronized void destroy() throws Exception {
        for (SimpleMessageListenerContainer l :listeners){
            l.stop();
            l.destroy();
        }
        listeners.clear();
    }

    private synchronized SimpleMessageListenerContainer addListener(final String queueName){

        final String bindQueueName =  context.getEnvironment().getProperty("rabbit.rpc." + queueName + ".queue");

        final Queue queue = new Queue(bindQueueName !=null ? bindQueueName : queueName);
        admin.declareQueue(queue);

        if (bindQueueName == null){
            //Автоматически создавать exchange/binding только для конфигурации по-умолчанию

            final String exchangeName = queueName;

            if (!isExchangeExists(exchangeName)){
                log.info("Creating exchange '{}'", exchangeName);
                final FanoutExchange exchange = new FanoutExchange(exchangeName);
                admin.declareExchange(exchange);

                admin.declareBinding(BindingBuilder.bind(queue).to(exchange));
            }else{
                log.debug("Exchange '{}' has been allready existed");
            }
        }

        final SimpleMessageListenerContainer l  = (SimpleMessageListenerContainer)context.getBean("thriftRabbitMessageListener", queue.getName(), cf, listener);
        listeners.add(l);
        return l;
    }

    public boolean isExchangeExists(final String exchange){
        return admin.getRabbitTemplate().execute(new ChannelCallback<DeclareOk>() {
            @Override
            public DeclareOk doInRabbit(Channel channel) throws Exception {
                try {
                    return channel.exchangeDeclarePassive(exchange);
                }
                catch (Exception e) {
                    return null;
                }
            }
        }) != null;
    }

    @Override
    public <T> T onIface(Class<T> cls) {
        return rabbitThriftClient.onIface(cls);
    }

}
