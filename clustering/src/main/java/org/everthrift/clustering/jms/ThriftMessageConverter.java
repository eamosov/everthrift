package org.everthrift.clustering.jms;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.everthrift.clustering.thrift.ThriftCallFuture;
import org.springframework.jms.support.converter.MessageConversionException;
import org.springframework.jms.support.converter.MessageConverter;

import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Session;

/**
 * Created by fluder on 16/08/17.
 */
public class ThriftMessageConverter implements MessageConverter {

    static private TProtocolFactory protocolFactory = new TBinaryProtocol.Factory();

    @SuppressWarnings("rawtypes")
    @Override
    public Message toMessage(Object object, Session session) throws JMSException, MessageConversionException {

        if (!(object instanceof ThriftCallFuture)) {
            throw new MessageConversionException("coudn't convert class: " + object.getClass().getSimpleName());
        }

        final BytesMessage bm = session.createBytesMessage();
        final ThriftCallFuture ii = (ThriftCallFuture) object;

        bm.setStringProperty("method", ii.getFullMethodName());
        bm.setStringProperty("args", ii.args.toString());

        ii.serializeCall(0, new TTransport() {

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
}
