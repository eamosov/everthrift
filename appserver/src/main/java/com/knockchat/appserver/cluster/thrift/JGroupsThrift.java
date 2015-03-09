package com.knockchat.appserver.cluster.thrift;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.thrift.TApplicationException;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TMemoryBuffer;
import org.jgroups.Address;
import org.jgroups.Message;
import org.jgroups.blocks.RequestOptions;
import org.jgroups.blocks.ResponseMode;
import org.jgroups.util.Rsp;
import org.jgroups.util.RspList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.knockchat.appserver.cluster.JgroupsMessageDispatcher;
import com.knockchat.appserver.controller.MessageWrapper;
import com.knockchat.utils.thrift.InvocationInfo;
import com.knockchat.utils.thrift.InvocationInfoThreadHolder;

@Component
public class JGroupsThrift {
	
	private static final Logger log = LoggerFactory.getLogger(JGroupsThrift.class);
	
	@Autowired
	private JgroupsMessageDispatcher jgroupsMessageDispatcher;
	
	private final TProtocolFactory binaryProtocolFactory = new TBinaryProtocol.Factory();
	
	public <T> Map<Address, T> thriftCall(Collection<Address> dest, int timeout, int seqId, T methodCall) throws TException{
		final InvocationInfo info = InvocationInfoThreadHolder.getInvocationInfo();
		if (info == null)
			throw new TException("info is NULL");
		
		return thriftCall(dest, null, timeout, seqId, ResponseMode.GET_ALL, info);
	}
	
	public <T> Map<Address, T> thriftCall(Collection<Address> dest, Collection<Address> exclusionList, int timeout, int seqId, ResponseMode responseMode, InvocationInfo tInfo) throws TException{
		
				
		final Message msg = new Message();
		msg.setObject(new MessageWrapper(tInfo.buildCall(seqId, binaryProtocolFactory)));
				
		final RequestOptions options = new RequestOptions(responseMode, timeout);
		
		if (exclusionList!=null){
			options.setExclusionList(exclusionList.toArray(new Address[exclusionList.size()]));
		}
		
		RspList<MessageWrapper> resp;
		try {
			resp = jgroupsMessageDispatcher.getMessageDispatcher().castMessage(dest, msg, options);
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
