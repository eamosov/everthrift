package com.knockchat.appserver.transport.jms;

import java.lang.reflect.Proxy;
import java.util.List;
import java.util.Set;

import javax.jms.BytesMessage;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Session;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.jms.listener.AbstractMessageListenerContainer;
import org.springframework.jms.listener.DefaultMessageListenerContainer;
import org.springframework.jms.listener.SessionAwareMessageListener;
import org.springframework.jms.support.converter.MessageConversionException;
import org.springframework.jms.support.converter.MessageConverter;
import org.springframework.stereotype.Component;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.knockchat.appserver.controller.ThriftControllerInfo;
import com.knockchat.appserver.controller.ThriftProcessor;
import com.knockchat.appserver.controller.ThriftProcessorFactory;
import com.knockchat.utils.thrift.InvocationCallback;
import com.knockchat.utils.thrift.InvocationInfo;
import com.knockchat.utils.thrift.NullResult;
import com.knockchat.utils.thrift.ServiceIfaceProxy;
import com.knockchat.utils.thrift.ThriftProxyFactory;

@Component
@Configuration
public class JmsThriftAdapter implements InitializingBean, DisposableBean{
	
	private final static Logger log = LoggerFactory.getLogger(JmsThriftAdapter.class);
	
	private JmsTemplate jmsTemplate;
    
    @Autowired
    private ConnectionFactory jmsConnectionFactory;
    
    @Autowired
    private ApplicationContext context;
    
	@Autowired
	private RpcJmsRegistry rpcJmsRegistry;

	@Autowired
	private ThriftProcessorFactory thriftProcessorFactory;
	private ThriftProcessor thriftProcessor;

    private final TProtocolFactory binaryProtocolFactory = new TBinaryProtocol.Factory();
    
    private List<AbstractMessageListenerContainer> listeners = Lists.newArrayList();
    
    private SessionAwareMessageListener<Message> listener = new SessionAwareMessageListener<Message>(){

		@Override
		public void onMessage(final Message message, Session session) throws JMSException {
			
			log.debug("onMessage:{}", message);
					
			if (!(message instanceof BytesMessage))
				throw new JMSException("invalid message class: " + message.getClass().getSimpleName());
			
			
			try {
				thriftProcessor.process(
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
									try {
										if (off == 0){
											return ((BytesMessage)message).readBytes(buf, len);
										}else{
											final byte b[] = new byte[len];
											final int r = ((BytesMessage)message).readBytes(b, len);
											if (r>0)
												System.arraycopy(b, 0, buf, off, r);
											return r;
										}
									} catch (JMSException e) {
										throw new TTransportException(e);
									}
								}
				
								@Override
								public void write(byte[] buf, int off, int len) throws TTransportException {
									throw new TTransportException("not implemented");
								}
							}),
					
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
			} catch (TException e) {
				
				if (e.getCause() instanceof JMSException)
					throw (JMSException)e.getCause();
				
				final JMSException je = new JMSException(e.getMessage());
				je.setLinkedException(e);
				throw je;
			}
		}};

	@SuppressWarnings("unchecked")
	public <T> T onIface(Class<T> cls){
		
		return (T)Proxy.newProxyInstance(ThriftProxyFactory.class.getClassLoader(), new Class[]{cls}, new ServiceIfaceProxy(cls, new InvocationCallback(){

			@SuppressWarnings("rawtypes")
			@Override
			public Object call(InvocationInfo ii) throws NullResult {
				jmsTemplate.convertAndSend(ii.serviceName, ii);
				throw new NullResult();
			}}));
	}
	
	@Override
	public void afterPropertiesSet() throws Exception {

		thriftProcessor = thriftProcessorFactory.getThriftProcessor(rpcJmsRegistry, new TBinaryProtocol.Factory());
		
		final Set<String> services = Sets.newHashSet();
		for (ThriftControllerInfo i:rpcJmsRegistry.getControllers().values())
			services.add(i.getServiceName());
				
		for(String s: services)
			addJmsListener(s);
		
		jmsTemplate = new JmsTemplate(jmsConnectionFactory);
		jmsTemplate.setMessageConverter(new MessageConverter(){

			@SuppressWarnings("rawtypes")
			@Override
			public Message toMessage(Object object, Session session) throws JMSException, MessageConversionException {
				
				if (!(object instanceof InvocationInfo))
					throw new MessageConversionException("coudn't convert class: " + object.getClass().getSimpleName());
				
				final BytesMessage bm = session.createBytesMessage();
				final InvocationInfo ii = (InvocationInfo) object;
				
				bm.setStringProperty("method", ii.fullMethodName);
				bm.setStringProperty("args", ii.args.toString());
				
				ii.buildCall(0, new TTransport(){

					@Override
					public boolean isOpen() {return true;}

					@Override
					public void open() throws TTransportException {}

					@Override
					public void close() {}

					@Override
					public int read(byte[] buf, int off, int len) throws TTransportException { throw new TTransportException("not implemented");}

					@Override
					public void write(byte[] buf, int off, int len) throws TTransportException {
						try {
							bm.writeBytes(buf, off, len);
						} catch (JMSException e) {
							throw new TTransportException(e);
						}
					}}, binaryProtocolFactory);
				
				return bm;
			}

			@Override
			public Object fromMessage(Message message) throws JMSException, MessageConversionException {
				// TODO Auto-generated method stub
				return null;
			}});
	}
	
    @Bean
    @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)    
    public DefaultMessageListenerContainer thriftJmsMessageListener(String destination, ConnectionFactory connectionFactory, SessionAwareMessageListener listener){
    	final DefaultMessageListenerContainer container = new DefaultMessageListenerContainer();
    	
    	container.setConnectionFactory(connectionFactory);
    	container.setDestinationName(destination);
    	container.setMessageListener(listener);
    	container.setSessionTransacted(true);    	
    	container.start();
    	
    	return container;
    }
	
	private synchronized DefaultMessageListenerContainer addJmsListener(String queueName){
		DefaultMessageListenerContainer l  = (DefaultMessageListenerContainer)context.getBean("thriftJmsMessageListener", queueName, jmsConnectionFactory, listener);        
		listeners.add(l);
		return l;
	}

	@Override
	public synchronized void destroy() throws Exception {
		for (AbstractMessageListenerContainer l :listeners){
			l.stop();
			l.destroy();
		}
		listeners.clear();
	}

}
