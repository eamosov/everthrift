package org.everthrift.appserver.jgroups;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.Resource;

import org.apache.commons.lang.NotImplementedException;
import org.everthrift.clustering.MessageWrapper;
import org.everthrift.clustering.jgroups.AbstractJgroupsThriftClientImpl;
import org.everthrift.clustering.jgroups.ClusterThriftClientIF;
import org.everthrift.clustering.thrift.InvocationInfoThreadHolder;
import org.everthrift.clustering.thrift.ThriftProxyFactory;
import org.everthrift.services.thrift.cluster.ClusterService;
import org.jgroups.Address;
import org.jgroups.JChannel;
import org.jgroups.MembershipListener;
import org.jgroups.Message;
import org.jgroups.View;
import org.jgroups.blocks.AsyncRequestHandler;
import org.jgroups.blocks.MessageDispatcher;
import org.jgroups.blocks.ResponseMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHeaders;
import org.springframework.scheduling.annotation.Scheduled;


public class JgroupsThriftClientServerImpl extends AbstractJgroupsThriftClientImpl implements AsyncRequestHandler, InitializingBean, MembershipListener, ClusterThriftClientIF {
	
	private static final Logger log = LoggerFactory.getLogger(JgroupsThriftClientServerImpl.class);

	private final JChannel cluster;
	
	@Autowired
	private ApplicationContext applicationContext;
	
	@Autowired
	private RpcJGroupsRegistry rpcJGroupsRegistry;
	
	private MessageDispatcher  disp;
	
	private List<MembershipListener> membershipListeners = new ArrayList<MembershipListener>();
		
	@Resource
	private MessageChannel inJGroupsChannel;

	public JgroupsThriftClientServerImpl(JChannel cluster){
		log.info("Using {} as MulticastThriftTransport", this.getClass().getSimpleName());
		this.cluster = cluster;
	}

	@Override
	public Object handle(Message msg) throws Exception {
		log.error("NotImplemented");
		throw new NotImplementedException();
	}
	
	@Override
	public void handle(Message request, org.jgroups.blocks.Response response) throws Exception {
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
		
		disp=new MessageDispatcher(cluster, null, this, this);
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
		
		if (!this.viewAccepted.isDone())
			this.viewAccepted.set(null);
		
		nodeDb.retain(new_view.getMembers());
		populateConfiguration();
		
		for(MembershipListener m: membershipListeners){
			m.viewAccepted(new_view);
		}		
	}

	@Override
	public synchronized void suspect(Address suspected_mbr) {
		
		populateConfiguration();
		
		for(MembershipListener m: membershipListeners){
			m.suspect(suspected_mbr);
		}		
	}

	@Override
	public synchronized void block() {
		
		populateConfiguration();
		
		for(MembershipListener m: membershipListeners){
			m.block();
		}		
	}

	@Override
	public synchronized void unblock() {
		
		populateConfiguration();
		
		for(MembershipListener m: membershipListeners){
			m.unblock();
		}		
	}
	
	@Scheduled(fixedRate=5000)
	public void logClusterState(){
		log.info("cluster:{}", cluster.getView());
		
		populateConfiguration();
	}

	@Override
	public JChannel getCluster() {
		return cluster;
	}
	
	public void populateConfiguration(){		
		try {
			ThriftProxyFactory.on(ClusterService.Iface.class).onNodeConfiguration(rpcJGroupsRegistry.getNodeConfiguration());
			call(InvocationInfoThreadHolder.getInvocationInfo(), ClusterThriftClientIF.Options.responseMode(ResponseMode.GET_NONE));
		} catch (Exception e) {
			log.error("Exception", e);
		}		
	}	
}
