package org.everthrift.rabbit;

import org.everthrift.appserver.controller.ThriftProcessor;
import org.everthrift.appserver.transport.rabbit.RpcRabbit;
import org.everthrift.clustering.thrift.ThriftControllerDiscovery;
import org.everthrift.thrift.ThriftServicesDiscovery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.MessageListener;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;

@Configuration
public class RabbitConfig {

    private final static Logger log = LoggerFactory.getLogger(RabbitConfig.class);

    @Bean
    public ConnectionFactory rabbitConnectionFactory(@Value("${rabbit.host}") String rabbitHost,
                                                     @Value("${rabbit.port:5672}") String rabbitPort) {

        log.info("Starting bean: o.s.a.r.c.ConnectionFactory({}:{})", rabbitHost, rabbitPort);
        return new CachingConnectionFactory(rabbitHost, Integer.parseInt(rabbitPort));
    }

    @Bean
    @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    public SimpleMessageListenerContainer thriftRabbitMessageListener(String destination,
                                                                      ConnectionFactory connectionFactory,
                                                                      MessageListener listener,
                                                                      int concurrentConsumers) {

        final SimpleMessageListenerContainer container = new SimpleMessageListenerContainer();

        container.setConnectionFactory(connectionFactory);
        container.setQueueNames(destination);
        container.setMessageListener(listener);
        container.setConcurrentConsumers(concurrentConsumers);
        container.start();

        return container;
    }

    @Bean
    public ThriftProcessor rabbitThriftProcessor() {
        return new ThriftProcessor(RpcRabbit.class);
    }

    @Bean
    public RabbitThriftClientServerImpl rabbitThriftClientServerImpl(ConnectionFactory connectionFactory,
                                                                     @Value("${rabbit.queue.prefix:}")
                                                                         String queuePrefix,
                                                                     @Value("${rabbit.queue.suffix:}")
                                                                         String queueSuffix,
                                                                     @Value("${rabbit.exchange.prefix:}")
                                                                         String exchangePrefix,
                                                                     @Value("${rabbit.exchange.suffix:}")
                                                                         String exchangeSuffix,
                                                                     @Qualifier("rabbitThriftProcessor") ThriftProcessor rabbitThriftProcessor,
                                                                     ApplicationContext context,
                                                                     ThriftServicesDiscovery thriftServicesDiscovery,
                                                                     ThriftControllerDiscovery thriftControllerDiscovery

    ) {
        final RabbitThriftClientServerImpl r = new RabbitThriftClientServerImpl(connectionFactory,
                                                                                rabbitThriftProcessor,
                                                                                context,
                                                                                thriftServicesDiscovery,
                                                                                thriftControllerDiscovery);
        r.setQueuePrefix(queuePrefix);
        r.setQueueSuffix(queueSuffix);
        r.setExchangePrefix(exchangePrefix);
        r.setExchangeSuffix(exchangeSuffix);
        return r;
    }

}
