package com.knockchat.appserver.cluster;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Map;

import javax.annotation.PostConstruct;

import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TMemoryBuffer;
import org.jgroups.Address;
import org.jgroups.blocks.ResponseMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.knockchat.appserver.controller.ThriftProcessor;
import com.knockchat.appserver.transport.jgroups.RpcJGroupsRegistry;
import com.knockchat.utils.thrift.InvocationInfo;
import com.knockchat.utils.thrift.InvocationInfoThreadHolder;

public class LoopbackClusterThriftTransport implements MulticastThriftTransport {

	private static final Logger log = LoggerFactory.getLogger(LoopbackClusterThriftTransport.class);
	
	@Autowired
	private ApplicationContext applicationContext;
	
	@Autowired
	private RpcJGroupsRegistry rpcJGroupsRegistry;
	
	private TProcessor thriftProcessor;
	
	private final TProtocolFactory binary = new TBinaryProtocol.Factory();

	public LoopbackClusterThriftTransport() {
		log.info("Using {} as MulticastThriftTransport", this.getClass().getSimpleName());
	}

	@Override
	public <T> ListenableFuture<Map<Address, T>> thriftCall(boolean loopBack, int timeout, int seqId, ResponseMode responseMode, InvocationInfo ii) throws TException {
		if (loopBack == false)
			return Futures.immediateFuture(Collections.emptyMap());
		
		final TMemoryBuffer in = ii.buildCall(seqId, binary);
		final TProtocol inP = binary.getProtocol(in);
		final TMemoryBuffer out = new TMemoryBuffer(1024);
		final TProtocol outP = binary.getProtocol(out);
		
		thriftProcessor.process(inP, outP);
		return Futures.immediateFuture(Collections.singletonMap(new Address(){

			@Override
			public void writeTo(DataOutput out) throws Exception {
			}

			@Override
			public void readFrom(DataInput in) throws Exception {
			}

			@Override
			public int compareTo(Address o) {
				return 0;
			}

			@Override
			public void writeExternal(ObjectOutput out) throws IOException {
			}

			@Override
			public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
			}

			@Override
			public int size() {
				return 0;
			}}, (T)ii.setReply(out, binary)));
	}

	@Override
	public <T> ListenableFuture<Map<Address, T>> thriftCall(boolean loopBack, int timeout, int seqId, ResponseMode responseMode, T methodCall) throws TException {
		return thriftCall(loopBack, timeout, seqId, responseMode, InvocationInfoThreadHolder.getInvocationInfo());
	}

	@PostConstruct
	private void postConstruct(){
		thriftProcessor = ThriftProcessor.create(applicationContext, rpcJGroupsRegistry, new TBinaryProtocol.Factory());		
	}

	public TProcessor getThriftProcessor() {
		return thriftProcessor;
	}

	public void setThriftProcessor(TProcessor thriftProcessor) {
		this.thriftProcessor = thriftProcessor;
	}
}
