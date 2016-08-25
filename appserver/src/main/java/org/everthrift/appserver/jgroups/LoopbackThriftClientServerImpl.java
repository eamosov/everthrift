package org.everthrift.appserver.jgroups;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import javax.annotation.PostConstruct;

import org.apache.commons.lang.NotImplementedException;
import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TMemoryBuffer;
import org.everthrift.appserver.controller.ThriftProcessor;
import org.everthrift.clustering.jgroups.ClusterThriftClientImpl;
import org.everthrift.clustering.thrift.InvocationInfo;
import org.jgroups.Address;
import org.jgroups.JChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

public class LoopbackThriftClientServerImpl extends ClusterThriftClientImpl {

    private static final Logger log = LoggerFactory.getLogger(LoopbackThriftClientServerImpl.class);

    @Autowired
    private ApplicationContext applicationContext;

    @Autowired
    private RpcJGroupsRegistry rpcJGroupsRegistry;

    private TProcessor thriftProcessor;

    private final TProtocolFactory binary = new TBinaryProtocol.Factory();

    public LoopbackThriftClientServerImpl() {
        log.info("Using {} as MulticastThriftTransport", this.getClass().getSimpleName());
    }

    @Override
    public <T> ListenableFuture<Map<Address, Reply<T>>> call(Collection<Address> dest, Collection<Address> exclusionList,
                                                             InvocationInfo tInfo, Options... options) throws TException {

        if (isLoopback(options) == false)
            return Futures.immediateFuture(Collections.emptyMap());

        final TMemoryBuffer in = tInfo.buildCall(0, binary);
        final TProtocol inP = binary.getProtocol(in);
        final TMemoryBuffer out = new TMemoryBuffer(1024);
        final TProtocol outP = binary.getProtocol(out);

        thriftProcessor.process(inP, outP);
        return Futures.immediateFuture(Collections.singletonMap(new Address() {

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
            }
        }, new ReplyImpl<T>(() -> (T) tInfo.setReply(out, binary))));
    }

    @PostConstruct
    private void postConstruct() {
        thriftProcessor = ThriftProcessor.create(applicationContext, rpcJGroupsRegistry);
    }

    public TProcessor getThriftProcessor() {
        return thriftProcessor;
    }

    public void setThriftProcessor(TProcessor thriftProcessor) {
        this.thriftProcessor = thriftProcessor;
    }

    @Override
    public JChannel getCluster() {
        throw new NotImplementedException();
    }

}
