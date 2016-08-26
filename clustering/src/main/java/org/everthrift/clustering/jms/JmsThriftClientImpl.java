package org.everthrift.clustering.jms;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.everthrift.clustering.thrift.InvocationCallback;
import org.everthrift.clustering.thrift.InvocationInfo;
import org.everthrift.clustering.thrift.NullResult;
import org.everthrift.clustering.thrift.ServiceIfaceProxy;
import org.everthrift.clustering.thrift.ThriftProxyFactory;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.jms.support.converter.MessageConversionException;
import org.springframework.jms.support.converter.MessageConverter;

import javax.jms.BytesMessage;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Session;
import java.lang.reflect.Proxy;

public class JmsThriftClientImpl implements JmsThriftClientIF {

    private JmsTemplate jmsTemplate;

    private TProtocolFactory protocolFactory = new TBinaryProtocol.Factory();

    public JmsThriftClientImpl(final ConnectionFactory jmsConnectionFactory) {

        jmsTemplate = new JmsTemplate(jmsConnectionFactory);
        jmsTemplate.setMessageConverter(new MessageConverter() {

            @SuppressWarnings("rawtypes")
            @Override
            public Message toMessage(Object object, Session session) throws JMSException, MessageConversionException {

                if (!(object instanceof InvocationInfo)) {
                    throw new MessageConversionException("coudn't convert class: " + object.getClass().getSimpleName());
                }

                final BytesMessage bm = session.createBytesMessage();
                final InvocationInfo ii = (InvocationInfo) object;

                bm.setStringProperty("method", ii.fullMethodName);
                bm.setStringProperty("args", ii.args.toString());

                ii.buildCall(0, new TTransport() {

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
                        try {
                            bm.writeBytes(buf, off, len);
                        } catch (JMSException e) {
                            throw new TTransportException(e);
                        }
                    }
                }, protocolFactory);

                return bm;
            }

            @Override
            public Object fromMessage(Message message) throws JMSException, MessageConversionException {
                return null;
            }
        });

    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T onIface(Class<T> cls) {

        return (T) Proxy.newProxyInstance(ThriftProxyFactory.class.getClassLoader(), new Class[]{cls},
                                          new ServiceIfaceProxy(cls, new InvocationCallback() {

                                              @SuppressWarnings("rawtypes")
                                              @Override
                                              public Object call(InvocationInfo ii) throws NullResult {
                                                  jmsTemplate.convertAndSend(ii.serviceName, ii);
                                                  throw new NullResult();
                                              }
                                          }));
    }

}
