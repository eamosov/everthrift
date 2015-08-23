package com.knockchat.appserver.transport.asynctcp;

import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TMemoryBuffer;
import org.apache.thrift.transport.TMemoryInputTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.support.ChannelInterceptor;

import com.knockchat.appserver.controller.MessageWrapper;
import com.knockchat.appserver.controller.ThriftProcessor;
import com.knockchat.appserver.controller.ThriftProcessorFactory;

public class AsyncTcpThriftAdapter implements InitializingBean, ChannelInterceptor{
	
	public static Logger log = LoggerFactory.getLogger(AsyncTcpThriftAdapter.class);
	
	@Autowired
	private ApplicationContext applicationContext;
	
	@Autowired
	private RpcAsyncTcpRegistry registry;
	
	@Autowired
	private ThriftProcessorFactory tpf;
	private ThriftProcessor tp;
	
	private final TProtocolFactory protocolFactory;
	
	public AsyncTcpThriftAdapter(TProtocolFactory protocolFactory){
		this.protocolFactory = protocolFactory;
	}
	
	public Object handle(Message m){
						
		log.debug("{}, adapter={}, processor={}", new Object[]{m, this, tp});
		
		try{
			return tp.process(new MessageWrapper(new TMemoryInputTransport((byte[])m.getPayload())).setMessageHeaders(m.getHeaders()).setOutChannel(applicationContext.getBean("outChannel", MessageChannel.class)), null);
		} catch (Exception e) {
			log.error("Exception while execution thrift processor:", e);
			return null;
		}		
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		tp = tpf.getThriftProcessor(registry, protocolFactory);		
	}

	@Override
	public Message<?> preSend(Message<?> message, MessageChannel channel) {
		return MessageBuilder.withPayload(((TMemoryBuffer)((MessageWrapper)message.getPayload()).getTTransport()).toByteArray()).copyHeaders(message.getHeaders()).build();
	}

	@Override
	public void postSend(Message<?> message, MessageChannel channel, boolean sent) {
		
	}

	@Override
	public boolean preReceive(MessageChannel channel) {
		return true;
	}

	@Override
	public Message<?> postReceive(Message<?> message, MessageChannel channel) {
		return message;
	}

	@Override
	public void afterSendCompletion(Message<?> message, MessageChannel channel, boolean sent, Exception ex) {
	}

	@Override
	public void afterReceiveCompletion(Message<?> message, MessageChannel channel, Exception ex) {
	}
}
