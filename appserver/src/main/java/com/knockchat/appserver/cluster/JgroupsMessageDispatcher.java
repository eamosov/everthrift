package com.knockchat.appserver.cluster;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.Resource;

import org.apache.commons.lang.NotImplementedException;
import org.jgroups.Address;
import org.jgroups.JChannel;
import org.jgroups.MembershipListener;
import org.jgroups.Message;
import org.jgroups.View;
import org.jgroups.blocks.AsyncRequestHandler;
import org.jgroups.blocks.MessageDispatcher;
import org.jgroups.blocks.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHeaders;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.knockchat.appserver.controller.MessageWrapper;
import com.knockchat.appserver.transport.jgroups.JGroupsThriftAdapter;


@Component("jgroupsMessageDispatcher")
public class JgroupsMessageDispatcher implements AsyncRequestHandler, InitializingBean, MembershipListener {
	
	private static final Logger log = LoggerFactory.getLogger(JgroupsMessageDispatcher.class);

	@Resource
	private JChannel yocluster;
	
	@Autowired
	private ApplicationContext applicationContext;
	
	private MessageDispatcher  disp;
	
	private List<MembershipListener> membershipListeners = new ArrayList<MembershipListener>();
	
	@Resource
	private MessageChannel inJGroupsChannel;


	@Override
	public Object handle(Message msg) throws Exception {
		log.error("NotImplemented");
		throw new NotImplementedException();
	}
	
	@Override
	public void handle(Message request, Response response) throws Exception {
		log.debug("handle message: {}, {}", request, response);
		
		final MessageWrapper w = (MessageWrapper)request.getObject();
		w.setAttribute(JGroupsThriftAdapter.HEADER_JRESPONSE, response);
		w.setAttribute("src", request.getSrc());
				
		org.springframework.messaging.Message<MessageWrapper> m = MessageBuilder.<MessageWrapper>withPayload(w).setHeader(MessageHeaders.REPLY_CHANNEL, "outJGroupsChannel").build();		
		inJGroupsChannel.send(m);		
	}
	
	
	public void destroy() {
		yocluster.close();
	}
			
	public synchronized void addMembershipListener(MembershipListener m){
		membershipListeners.add(m);
	}

	public synchronized void removeMembershipListener(MembershipListener m){
		membershipListeners.remove(m);
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		log.info("Starting JgroupsMessageDispatcher");
		
		disp=new MessageDispatcher(yocluster, null, null, this);
		disp.asyncDispatching(true);

		yocluster.connect(applicationContext.getEnvironment().getProperty("cluster.name"));
	}
	
	public MessageDispatcher getMessageDispatcher(){
		return disp;
	}
	
	public Address getLocalAddress(){
		return yocluster.getAddress();
	}

	@Override
	public synchronized void viewAccepted(View new_view) {
		for(MembershipListener m: membershipListeners){
			m.viewAccepted(new_view);
		}		
	}

	@Override
	public synchronized void suspect(Address suspected_mbr) {
		for(MembershipListener m: membershipListeners){
			m.suspect(suspected_mbr);
		}		
	}

	@Override
	public synchronized void block() {
		for(MembershipListener m: membershipListeners){
			m.block();
		}		
	}

	@Override
	public synchronized void unblock() {
		for(MembershipListener m: membershipListeners){
			m.unblock();
		}		
	}
	
	@Scheduled(fixedRate=5000)
	public void logClusterState(){
		log.info("cluster:{}", yocluster.getView());
	}

}
