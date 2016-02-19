package com.knockchat.appserver.transport.jgroups;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Resource;

import org.apache.commons.lang.NotImplementedException;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.jgroups.Address;
import org.jgroups.JChannel;
import org.jgroups.MembershipListener;
import org.jgroups.Message;
import org.jgroups.View;
import org.jgroups.blocks.AsyncRequestHandler;
import org.jgroups.blocks.MessageDispatcher;
import org.jgroups.blocks.RequestOptions;
import org.jgroups.blocks.Response;
import org.jgroups.blocks.ResponseMode;
import org.jgroups.util.Rsp;
import org.jgroups.util.RspList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHeaders;
import org.springframework.scheduling.annotation.Scheduled;

import com.knockchat.appserver.cluster.MulticastThriftTransport;
import com.knockchat.appserver.controller.MessageWrapper;
import com.knockchat.utils.thrift.InvocationInfo;
import com.knockchat.utils.thrift.InvocationInfoThreadHolder;


public class JgroupsMessageDispatcher implements AsyncRequestHandler, InitializingBean, MembershipListener, MulticastThriftTransport {
	
	private static final Logger log = LoggerFactory.getLogger(JgroupsMessageDispatcher.class);

	private final JChannel cluster;
	
	@Autowired
	private ApplicationContext applicationContext;
	
	private MessageDispatcher  disp;
	
	private List<MembershipListener> membershipListeners = new ArrayList<MembershipListener>();
	
	@Resource
	private MessageChannel inJGroupsChannel;

	public JgroupsMessageDispatcher(JChannel cluster){
		this.cluster = cluster;
	}

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
		cluster.close();
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
		
		disp=new MessageDispatcher(cluster, null, null, this);
		disp.asyncDispatching(true);

		cluster.connect(applicationContext.getEnvironment().getProperty("jgroups.cluster.name"));
	}
	
	public MessageDispatcher getMessageDispatcher(){
		return disp;
	}
	
	public Address getLocalAddress(){
		return cluster.getAddress();
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
		log.info("cluster:{}", cluster.getView());
	}

	private final TProtocolFactory binaryProtocolFactory = new TBinaryProtocol.Factory();
	
	@Override
	public <T> Map<Address, T> thriftCall(boolean loopBack, int timeout, int seqId, ResponseMode responseMode, final InvocationInfo ii) throws TException{
		return thriftCall(null, loopBack ? null : Collections.singleton(getLocalAddress()), timeout,  seqId, responseMode, ii);
	}

	@Override
	public <T> Map<Address, T> thriftCall(boolean loopBack, int timeout, int seqId, ResponseMode responseMode, T methodCall) throws TException{
		return thriftCall(null, loopBack ? null : Collections.singleton(getLocalAddress()), timeout,  seqId, responseMode, InvocationInfoThreadHolder.getInvocationInfo());
	}

	public <T> Map<Address, T> thriftCall(Collection<Address> dest, int timeout, int seqId, T methodCall) throws TException{
		final InvocationInfo info = InvocationInfoThreadHolder.getInvocationInfo();
		if (info == null)
			throw new TException("info is NULL");
		
		return thriftCall(dest, null, timeout, seqId, ResponseMode.GET_ALL, info);
	}
	
//	public <T> Map<Address, T> thriftCall(int timeout, int seqId, ResponseMode responseMode, InvocationInfo tInfo) throws TException{
//		return thriftCall(null, null, timeout, seqId, responseMode, tInfo);
//	}
	
	public <T> Map<Address, T> thriftCall(Collection<Address> dest, Collection<Address> exclusionList, int timeout, int seqId, ResponseMode responseMode, InvocationInfo tInfo) throws TException{
		
				
		final Message msg = new Message();
		msg.setObject(new MessageWrapper(tInfo.buildCall(seqId, binaryProtocolFactory)));
				
		final RequestOptions options = new RequestOptions(responseMode, timeout);
		
		if (exclusionList!=null){
			options.setExclusionList(exclusionList.toArray(new Address[exclusionList.size()]));
		}
		
		RspList<MessageWrapper> resp;
		try {
			resp = disp.castMessage(dest, msg, options);
		} catch (Exception e) {
			log.error("Exception in getConfiguration", e);
			return null;
		}
		
		log.trace("RspList:{}", resp);

		if (responseMode == ResponseMode.GET_NONE){
			return null;
		}
		
		final Map<Address, T> ret = new HashMap<Address, T>();		
		
		for (Rsp<MessageWrapper>responce: resp){
			if (responce.getValue() !=null){
				
				try{
					
					final T success = (T)tInfo.setReply(responce.getValue().getTTransport(), binaryProtocolFactory);					
					ret.put(responce.getSender(), success);
					
				} catch (TApplicationException e) {
					if (e.getType() == TApplicationException.UNKNOWN_METHOD){
						log.debug("UNKNOWN_METHOD from {}:{}", responce.getSender(), e.getMessage());
					}else{
						log.error("Exception while reading thrift answer from " + responce.getSender(), e);
					}
				} catch (Exception e) {
					log.error("Exception while reading thrift answer from " + responce.getSender(), e);
				}
				
			}else{
				log.warn("null responce from {}", responce.getSender());
			}
		}
		
		return ret;
	}

}
