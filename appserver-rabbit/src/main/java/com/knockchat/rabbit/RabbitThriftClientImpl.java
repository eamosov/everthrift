package com.knockchat.rabbit;

import java.lang.reflect.Proxy;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TMemoryBuffer;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.converter.MessageConversionException;
import org.springframework.amqp.support.converter.MessageConverter;

import com.knockchat.clustering.thrift.InvocationCallback;
import com.knockchat.clustering.thrift.InvocationInfo;
import com.knockchat.clustering.thrift.NullResult;
import com.knockchat.clustering.thrift.ServiceIfaceProxy;
import com.knockchat.clustering.thrift.ThriftProxyFactory;

public class RabbitThriftClientImpl implements RabbitThriftClientIF {
	
	private RabbitTemplate rabbitTemplate;
	private TProtocolFactory  protocolFactory = new TBinaryProtocol.Factory();
	
	public RabbitThriftClientImpl(final ConnectionFactory rabbitConnectionFactory){
		
		rabbitTemplate = new RabbitTemplate(rabbitConnectionFactory);
		rabbitTemplate.setMessageConverter(new MessageConverter(){

			@SuppressWarnings("rawtypes")
			@Override
			public Message toMessage(Object object, MessageProperties messageProperties) throws MessageConversionException {
				
				if (!(object instanceof InvocationInfo))
					throw new MessageConversionException("coudn't convert class: " + object.getClass().getSimpleName());
				
				final InvocationInfo ii = (InvocationInfo) object;
				
//				bm.setStringProperty("method", ii.fullMethodName);
//				bm.setStringProperty("args", ii.args.toString());
				
				final TMemoryBuffer bytes = ii.buildCall(0, protocolFactory);								
				return new Message(bytes.toByteArray(), messageProperties);
			}

			@Override
			public Object fromMessage(Message message) throws MessageConversionException {

				return null;
			}});
		
	}
	

	@Override
	@SuppressWarnings("unchecked")	
	public <T> T onIface(Class<T> cls){
		
		return (T)Proxy.newProxyInstance(ThriftProxyFactory.class.getClassLoader(), new Class[]{cls}, new ServiceIfaceProxy(cls, new InvocationCallback(){

			@SuppressWarnings("rawtypes")
			@Override
			public Object call(InvocationInfo ii) throws NullResult {
				rabbitTemplate.convertAndSend(ii.serviceName, ii.methodName, ii);
				throw new NullResult();
			}}));
	}
	
}
