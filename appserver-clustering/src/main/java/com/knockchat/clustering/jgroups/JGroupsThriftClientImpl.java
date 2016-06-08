package com.knockchat.clustering.jgroups;

import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TMessage;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.jgroups.Address;
import org.jgroups.JChannel;
import org.jgroups.MembershipListener;
import org.jgroups.Message;
import org.jgroups.View;
import org.jgroups.blocks.MessageDispatcher;
import org.jgroups.blocks.RequestHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;

import com.google.common.collect.Maps;
import com.knockchat.appserver.thrift.cluster.ClusterService;
import com.knockchat.appserver.thrift.cluster.Node;
import com.knockchat.clustering.MessageWrapper;

public class JGroupsThriftClientImpl extends AbstractJgroupsThriftClientImpl implements  MembershipListener, ClusterThriftClientIF {
	
	private static final Logger log = LoggerFactory.getLogger(JGroupsThriftClientImpl.class);

	private final JChannel cluster;
	private final String clusterName; 
		
	private MessageDispatcher  disp;
	
	private final TProtocolFactory protocolFactory = new TBinaryProtocol.Factory();
	
	private AtomicReference<Map<Address, Node>> nodes = new AtomicReference<Map<Address, Node>>(Maps.newHashMap());
	
	public JGroupsThriftClientImpl(String jgroupsXmlPath, String clusterName) throws Exception{
		this(new JChannel(jgroupsXmlPath), clusterName);
	}

	public JGroupsThriftClientImpl(JChannel cluster, String clusterName){
		log.info("Using {} as MulticastThriftTransport", this.getClass().getSimpleName());
		this.cluster = cluster;
		this.clusterName  = clusterName;
	}
	
	public void destroy() {
		cluster.close();
	}
			
	public void connect() throws Exception {
		log.info("Starting JGroups MessageDispatcher");
		
		disp=new MessageDispatcher(cluster, null, this, new RequestHandler(){

			@Override
			public Object handle(Message msg) throws Exception {
				
				final MessageWrapper w = (MessageWrapper)msg.getObject();				
				final TProtocol inp = protocolFactory.getProtocol(w.getTTransport());
				final TMessage m = inp.readMessageBegin();
				
				if ("ClusterService:onNodeConfiguration".equals(m.name) && m.type == 1 /*request*/){
					final ClusterService.onNodeConfiguration_args args = new ClusterService.onNodeConfiguration_args();
					args.read(inp);
					inp.readMessageEnd();
					
					setNode(msg.getSrc(), args.getNode());
				}						
					
				return null;
			}});

		cluster.connect(clusterName);
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
	}

	@Override
	public synchronized void suspect(Address suspected_mbr) {
	}

	@Override
	public synchronized void block() {
		
	}

	@Override
	public synchronized void unblock() {
		
	}
	
	@Scheduled(fixedRate=5000)
	public void logClusterState(){
		log.info("cluster:{}", cluster.getView());
	}

	@Override
	public JChannel getCluster() {
		return cluster;
	}


}
