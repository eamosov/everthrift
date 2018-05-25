package org.everthrift.clustering.rabbit;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TMemoryBuffer;
import org.everthrift.clustering.thrift.ThriftCallFuture;
import org.everthrift.clustering.thrift.NullResult;
import org.everthrift.clustering.thrift.ServiceIfaceProxy;
import org.everthrift.clustering.thrift.ThriftProxyFactory;
import org.everthrift.utils.ThriftServicesDb;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.converter.MessageConversionException;
import org.springframework.amqp.support.converter.MessageConverter;

import java.lang.reflect.Proxy;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class RabbitThriftClientImpl implements RabbitThriftClientIF {

    private RabbitTemplate rabbitTemplate;

    private static ExecutorService sendExecutor = Executors.newSingleThreadExecutor();

    private TProtocolFactory protocolFactory = new TBinaryProtocol.Factory();
    private final ThriftServicesDb thriftServicesDb;

    private String exchangePrefix = "";
    private String exchangeSuffix = "";

    public RabbitThriftClientImpl(final ConnectionFactory rabbitConnectionFactory, final ThriftServicesDb thriftServicesDb) {

        this.thriftServicesDb = thriftServicesDb;
        rabbitTemplate = new RabbitTemplate(rabbitConnectionFactory);
        rabbitTemplate.setMessageConverter(new MessageConverter() {

            @SuppressWarnings("rawtypes")
            @Override
            public Message toMessage(Object object, MessageProperties messageProperties) throws MessageConversionException {

                if (!(object instanceof ThriftCallFuture)) {
                    throw new MessageConversionException("coudn't convert class: " + object.getClass().getSimpleName());
                }

                final ThriftCallFuture ii = (ThriftCallFuture) object;

                // bm.setStringProperty("method", ii.fullMethodName);
                // bm.setStringProperty("args", ii.args.toString());

                final TMemoryBuffer bytes = ii.serializeCall(0, protocolFactory);
                return new Message(bytes.toByteArray(), messageProperties);
            }

            @Override
            public Object fromMessage(Message message) throws MessageConversionException {

                return null;
            }
        });

    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T onIface(Class<T> cls) {

        return (T) Proxy.newProxyInstance(ThriftProxyFactory.class.getClassLoader(), new Class[]{cls},
                                          new ServiceIfaceProxy(thriftServicesDb, ii -> {
                                              sendExecutor.execute(() -> {
                                                  rabbitTemplate.convertAndSend(getExchangeName(ii.thriftMethodEntry.serviceName), ii.thriftMethodEntry.methodName, ii);
                                              });
                                              throw new NullResult();
                                          }));
    }

    @Override
    public String getExchangeName(String serviceName) {
        return exchangePrefix + serviceName + exchangeSuffix;
    }

    public String getExchangePrefix() {
        return exchangePrefix;
    }

    public void setExchangePrefix(String exchangePrefix) {
        this.exchangePrefix = exchangePrefix;
    }

    public String getExchangeSuffix() {
        return exchangeSuffix;
    }

    public void setExchangeSuffix(String exchangeSuffix) {
        this.exchangeSuffix = exchangeSuffix;
    }

}
