package org.everthrift.clustering.jms;

import org.everthrift.clustering.thrift.NullResult;
import org.everthrift.clustering.thrift.ServiceIfaceProxy;
import org.everthrift.clustering.thrift.ThriftProxyFactory;
import org.everthrift.thrift.ThriftServicesDiscovery;
import org.springframework.jms.core.JmsTemplate;

import javax.jms.ConnectionFactory;
import java.lang.reflect.Proxy;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class JmsThriftClientImpl implements JmsThriftClientIF {


    private String queuePrefix = "";
    private String queueSuffix = "";

    private static ExecutorService sendExecutor = Executors.newSingleThreadExecutor();
    private static ThriftMessageConverter thriftMessageConverter = new ThriftMessageConverter();

    private final ConnectionFactory jmsConnectionFactory;
    private final ThriftServicesDiscovery thriftServicesDb;

    public JmsThriftClientImpl(final ConnectionFactory jmsConnectionFactory, ThriftServicesDiscovery thriftServicesDb) {
        this.jmsConnectionFactory = jmsConnectionFactory;
        this.thriftServicesDb = thriftServicesDb;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T onIface(Class<T> cls) {

        return (T) Proxy.newProxyInstance(ThriftProxyFactory.class.getClassLoader(), new Class[]{cls},
                                          new ServiceIfaceProxy(thriftServicesDb, ii -> {
                                              sendExecutor.execute(() -> {
                                                  final JmsTemplate jmsTemplate = new JmsTemplate(jmsConnectionFactory);
                                                  jmsTemplate.setMessageConverter(thriftMessageConverter);
                                                  jmsTemplate.convertAndSend(queuePrefix + ii.thriftMethodEntry.serviceName + queueSuffix, ii);
                                              });
                                              throw new NullResult();
                                          }));
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
}
