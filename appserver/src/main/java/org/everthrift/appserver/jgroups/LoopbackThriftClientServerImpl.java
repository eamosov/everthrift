package org.everthrift.appserver.jgroups;

import org.apache.commons.lang.NotImplementedException;
import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TMemoryBuffer;
import org.everthrift.clustering.thrift.ThriftControllerDiscovery;
import org.everthrift.clustering.jgroups.ClusterThriftClientImpl;
import org.everthrift.thrift.ThriftCallFuture;
import org.jetbrains.annotations.NotNull;
import org.jgroups.Address;
import org.jgroups.JChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class LoopbackThriftClientServerImpl extends ClusterThriftClientImpl {

    private static final Logger log = LoggerFactory.getLogger(LoopbackThriftClientServerImpl.class);

    private final TProcessor thriftProcessor;

    private final TProtocolFactory binary = new TBinaryProtocol.Factory();

    public LoopbackThriftClientServerImpl(ThriftControllerDiscovery thriftControllerDiscovery, TProcessor thriftProcessor) {
        super(thriftControllerDiscovery);
        this.thriftProcessor = thriftProcessor;
        log.info("Using {} as MulticastThriftTransport", this.getClass().getSimpleName());
    }

    @NotNull
    @Override
    public <T> CompletableFuture<Map<Address, Reply<T>>> call(Collection<Address> dest, Collection<Address> exclusionList,
                                                              @NotNull ThriftCallFuture tInfo, Map<String, Object> attributes, Options... options) throws TException {

        if (!isLoopback(options)) {
            return CompletableFuture.completedFuture(Collections.emptyMap());
        }

        final TMemoryBuffer in = tInfo.serializeCall(0, binary);
        final TProtocol inP = binary.getProtocol(in);
        final TMemoryBuffer out = new TMemoryBuffer(1024);
        final TProtocol outP = binary.getProtocol(out);

        thriftProcessor.process(inP, outP);
        return CompletableFuture.completedFuture(Collections.singletonMap(new Address() {

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
        }, new ReplyImpl<T>(() -> (T) tInfo.deserializeReply(out, binary))));
    }

    public TProcessor getThriftProcessor() {
        return thriftProcessor;
    }

    @NotNull
    @Override
    public JChannel getCluster() {
        throw new NotImplementedException();
    }

}
