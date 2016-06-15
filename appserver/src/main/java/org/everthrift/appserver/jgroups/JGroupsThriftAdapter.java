package org.everthrift.appserver.jgroups;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.everthrift.appserver.controller.DefaultTProtocolSupport;
import org.everthrift.appserver.controller.ThriftProcessor;
import org.everthrift.clustering.MessageWrapper;
import org.jgroups.blocks.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;

public class JGroupsThriftAdapter implements InitializingBean{
	
	public static Logger log = LoggerFactory.getLogger(JGroupsThriftAdapter.class);
	
	public static final String HEADER_JRESPONSE="jResponse";
	
	@Autowired
	private ApplicationContext applicationContext;
	
	@Autowired
	private RpcJGroupsRegistry rpcJGroupsRegistry;
	
	private final TProtocolFactory protocolFactory = new TBinaryProtocol.Factory();
	
	private ThriftProcessor thriftProcessor;
	
	public Object handleIn(Message<MessageWrapper> m){

		log.debug("handleIn: {}, adapter={}, processor={}", new Object[]{m, this, thriftProcessor});
				
		if (m.getHeaders().getReplyChannel() == null)
			log.warn("reply channel is null for message: {}", m);
		
		try {
			final MessageWrapper w = m.getPayload();
			w.setMessageHeaders(m.getHeaders());
			w.setOutChannel(applicationContext.getBean((String)m.getHeaders().getReplyChannel(), MessageChannel.class));
			
			return thriftProcessor.process(new DefaultTProtocolSupport(w, protocolFactory), null);
		} catch (Exception e) {
			log.error("Exception while execution thrift processor:", e);
			return null;
		}
	}
	
	public Object handleOut(Message<MessageWrapper> m) throws Exception{
		
		log.debug("handleOut: {}, adapter={}, processor={}", new Object[]{m, this, thriftProcessor});
		
		final MessageWrapper w = m.getPayload();
		
		final Response jResponse = (Response)w.removeAttribute(HEADER_JRESPONSE);
		
		if (jResponse!=null)
			jResponse.send(w, false);
		else
			log.debug("jResponse IS NULL, no answer has been sended");
			
		return null;
	}
	

	@Override
	public void afterPropertiesSet() throws Exception {
		thriftProcessor = ThriftProcessor.create(applicationContext, rpcJGroupsRegistry);		
	}

}
