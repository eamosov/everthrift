package org.everthrift.jms;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.ImportResource;
import org.springframework.context.annotation.Scope;
import org.springframework.jms.listener.DefaultMessageListenerContainer;
import org.springframework.jms.listener.SessionAwareMessageListener;

import javax.jms.ConnectionFactory;

@Configuration
@ImportResource("classpath:jms-beans.xml")
public class Jms {

    @Bean
    public RpcJmsRegistry RpcJmsRegistry() {
        return new RpcJmsRegistry();
    }

    @Bean
    @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    public DefaultMessageListenerContainer thriftJmsMessageListener(String destination, ConnectionFactory connectionFactory,
                                                                    SessionAwareMessageListener listener) {
        final DefaultMessageListenerContainer container = new DefaultMessageListenerContainer();

        container.setConnectionFactory(connectionFactory);
        container.setDestinationName(destination);
        container.setMessageListener(listener);
        container.setSessionTransacted(true);
        container.start();

        return container;
    }

    @Bean
    public JmsThriftClientServerImpl jmsThriftClientServerImpl(ConnectionFactory jmsConnectionFactory,
                                                               @Value("${activemq.queue.prefix:}") String queuePrefix,
                                                               @Value("${activemq.queue.suffix:}") String queueSuffix) {
        final JmsThriftClientServerImpl s = new JmsThriftClientServerImpl(jmsConnectionFactory);
        s.setQueuePrefix(queuePrefix);
        s.setQueueSuffix(queueSuffix);

        return s;
    }

}
