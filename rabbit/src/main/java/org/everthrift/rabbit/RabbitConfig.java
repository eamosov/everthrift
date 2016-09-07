package org.everthrift.rabbit;

import org.springframework.amqp.core.MessageListener;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;

@Configuration
public class RabbitConfig {

    @Bean
    public RpcRabbitRegistry rpcRabbitRegistry() {
        return new RpcRabbitRegistry();
    }

    @Bean
    public ConnectionFactory rabbitConnectionFactory(@Value("${rabbit.host}") String rabbitHost,
                                                     @Value("${rabbit.port:5672}") String rabbitPort) {
        return new CachingConnectionFactory(rabbitHost, Integer.parseInt(rabbitPort));
    }

    @Bean
    @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    public SimpleMessageListenerContainer thriftRabbitMessageListener(String destination, ConnectionFactory connectionFactory,
                                                                      MessageListener listener) {
        final SimpleMessageListenerContainer container = new SimpleMessageListenerContainer();

        container.setConnectionFactory(connectionFactory);
        container.setQueueNames(destination);
        container.setMessageListener(listener);
        container.start();

        return container;
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
                                                                         String exchangeSuffix) {
        final RabbitThriftClientServerImpl r = new RabbitThriftClientServerImpl(connectionFactory);
        r.setQueuePrefix(queuePrefix);
        r.setQueueSuffix(queueSuffix);
        r.setExchangePrefix(exchangePrefix);
        r.setExchangeSuffix(exchangeSuffix);
        return r;
    }

}
