package com.knockchat.appserver.transport.jgroups;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.jgroups.blocks.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.integration.Message;
import org.springframework.integration.MessageChannel;
import org.springframework.stereotype.Component;

import com.knockchat.appserver.controller.MessageWrapper;
import com.knockchat.appserver.controller.ThriftProcessor;
import com.knockchat.appserver.controller.ThriftProcessorFactory;

@Component("jGroupsThriftAdapter")
public class JGroupsThriftAdapter implements InitializingBean{
	
	public static Logger log = LoggerFactory.getLogger(JGroupsThriftAdapter.class);
	
	public static final String HEADER_JRESPONSE="jResponse";
	
	@Autowired
	private ApplicationContext applicationContext;
	
	@Autowired
	private RpcJGroupsRegistry intRegistry;
	
	@Autowired
	private ThriftProcessorFactory tpf;
	private ThriftProcessor tp;
	
	public Object handleIn(Message<MessageWrapper> m){

		log.debug("handleIn: {}, adapter={}, processor={}", new Object[]{m, this, tp});
				
		if (m.getHeaders().getReplyChannel() == null)
			log.warn("reply channel is null for message: {}", m);
		
		try {
			final MessageWrapper w = m.getPayload();
			w.setMessageHeaders(m.getHeaders());
			w.setOutChannel(applicationContext.getBean((String)m.getHeaders().getReplyChannel(), MessageChannel.class));
			
			return tp.process(w, null);
		} catch (Exception e) {
			log.error("Exception while execution thrift processor:", e);
			return null;
		}
	}
	
	public Object handleOut(Message<MessageWrapper> m) throws Exception{
		
		log.debug("handleOut: {}, adapter={}, processor={}", new Object[]{m, this, tp});
		
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
		tp = tpf.getThriftProcessor(intRegistry, new TBinaryProtocol.Factory());		
	}

}
