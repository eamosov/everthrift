package org.everthrift.thrift;

import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TMemoryBuffer;
import org.apache.thrift.transport.TMemoryInputTransport;
import org.apache.thrift.transport.TTransport;

import java.util.concurrent.CompletableFuture;

@SuppressWarnings("rawtypes")
public class ThriftCallFuture<T> extends CompletableFuture<T> {

    public final TBase args;
    public final ThriftServicesDiscovery.ThriftMethodEntry thriftMethodEntry;

    private int seqId;

    public ThriftCallFuture(ThriftCallFuture other) {
        super();
        this.args = other.args;
        this.thriftMethodEntry = other.thriftMethodEntry;
    }

    public ThriftCallFuture(ThriftServicesDiscovery.ThriftMethodEntry thriftMethodEntry, TBase args) {
        super();
        this.thriftMethodEntry = thriftMethodEntry;
        this.args = args;
    }

    public T deserializeReply(byte[] data, TProtocolFactory protocolFactory) throws TException {
        return deserializeReply(data, 0, data.length, protocolFactory);
    }

    public T deserializeReply(byte[] data, int offset, int length, TProtocolFactory protocolFactory) throws TException {
        return deserializeReply(new TMemoryInputTransport(data, offset, length), protocolFactory);
    }

    @SuppressWarnings("unchecked")
    public T deserializeReply(TTransport inT, TProtocolFactory protocolFactory) throws TException {
        try {
            final T ret = (T) this.thriftMethodEntry.deserializeReply(seqId, inT, protocolFactory);
            super.complete(ret);
            return ret;
        } catch (TException e) {
            super.completeExceptionally(e);
            throw e;
        }
    }

    public void setException(TException e) {
        super.completeExceptionally(e);
    }

    public String getFullMethodName() {
        return thriftMethodEntry.getFullMethodName();
    }

    public void serializeCall(int seqId, TTransport outT, TProtocolFactory protocolFactory) {
        this.seqId = seqId;
        thriftMethodEntry.serializeCall(seqId, args, outT, protocolFactory);
    }

    public TMemoryBuffer serializeCall(int seqId, TProtocolFactory protocolFactory) {
        this.seqId = seqId;
        return thriftMethodEntry.serializeCall(seqId, args, protocolFactory);
    }

    @Override
    public String toString() {
        return "ThriftCallFuture [" + thriftMethodEntry.getFullMethodName() + "(" + args + "), seqId=" + seqId + "]";
    }
}