package org.everthrift.jms;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQPrefetchPolicy;
import org.apache.activemq.RedeliveryPolicy;
import org.apache.activemq.pool.PooledConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.jms.listener.DefaultMessageListenerContainer;
import org.springframework.jms.listener.SessionAwareMessageListener;

import javax.jms.ConnectionFactory;
import java.util.List;

@Configuration
public class Jms {

    private static final Logger log = LoggerFactory.getLogger(Jms.class);

    @Bean(destroyMethod = "stop")
    public ConnectionFactory jmsFactory(@Value("${activemq.url}") String activemqUrl,
                                        @Value("${jms.redelivery.init.delay:5000}") long initialRedeliveryDelay,
                                        @Value("${jms.redelivery.count:5}") int maximumRedeliveries,
                                        @Value("${jms.prefetchCount:1}") int prefetchCount
    ) {

        final ActiveMQConnectionFactory acf = new ActiveMQConnectionFactory();

        final RedeliveryPolicy rp = new RedeliveryPolicy();
        rp.setInitialRedeliveryDelay(initialRedeliveryDelay);
        rp.setMaximumRedeliveries(maximumRedeliveries);
        rp.setUseExponentialBackOff(true);
        acf.setRedeliveryPolicy(rp);
        acf.setBrokerURL(activemqUrl);

        final ActiveMQPrefetchPolicy activeMQPrefetchPolicy = new ActiveMQPrefetchPolicy();
        activeMQPrefetchPolicy.setAll(prefetchCount);
        acf.setPrefetchPolicy(activeMQPrefetchPolicy);

        final PooledConnectionFactory factory = new PooledConnectionFactory();
        factory.setConnectionFactory(acf);
        return factory;
    }

    @Bean
    public JmsTemplate myJmsTemplate(@Qualifier("jmsFactory") ConnectionFactory connectionFactory) {
        final JmsTemplate t = new JmsTemplate();
        t.setConnectionFactory(connectionFactory);
        return t;
    }

    @Bean
    public RpcJmsRegistry RpcJmsRegistry() {
        return new RpcJmsRegistry();
    }

    @Bean
    @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    public DefaultMessageListenerContainer thriftJmsMessageListener(String destination,
                                                                    ConnectionFactory connectionFactory,
                                                                    SessionAwareMessageListener listener,
                                                                    int maxConcurrentConsumers) {

        log.info("Starting JMS listener for {} with {} consumers", destination, maxConcurrentConsumers);

        final DefaultMessageListenerContainer container = new DefaultMessageListenerContainer();

        container.setMaxConcurrentConsumers(maxConcurrentConsumers);
        container.setConnectionFactory(connectionFactory);
        container.setDestinationName(destination);
        container.setMessageListener(listener);
        container.setSessionTransacted(false);
        container.start();

        return container;
    }

    @Bean
    @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    public JmsContainerManager jmsContainerManager(DefaultMessageListenerContainer container, String name){
        return new JmsContainerManager(container, name);
    }

    @Bean
    public JmsThriftClientServerImpl jmsThriftClientServerImpl(@Qualifier("jmsFactory") ConnectionFactory connectionFactory,
                                                               @Value("${activemq.queue.prefix:}") String queuePrefix,
                                                               @Value("${activemq.queue.suffix:}") String queueSuffix) {
        final JmsThriftClientServerImpl s = new JmsThriftClientServerImpl(connectionFactory);
        s.setQueuePrefix(queuePrefix);
        s.setQueueSuffix(queueSuffix);

        return s;
    }

}
