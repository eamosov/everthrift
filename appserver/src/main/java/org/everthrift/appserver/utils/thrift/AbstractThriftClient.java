package org.everthrift.appserver.utils.thrift;

import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.everthrift.clustering.thrift.InvocationInfo;
import org.everthrift.clustering.thrift.InvocationInfoThreadHolder;

import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;

public abstract class AbstractThriftClient<S> extends ThriftClient<S> {

    public AbstractThriftClient(S sessionId) {
        super(sessionId);
    }

    @Override
    public <T> CompletableFuture<T> thriftCall(int timeout, T methodCall, BiConsumer<? super T, ? super Throwable> callback) throws TException {
        final CompletableFuture<T> lf = thriftCall(timeout, methodCall);
        lf.whenComplete(callback);
        return lf;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public <T> CompletableFuture<T> thriftCall(int timeout, T methodCall) throws TException {
        final InvocationInfo info = InvocationInfoThreadHolder.getInvocationInfo();
        if (info == null) {
            throw new TTransportException("info is NULL");
        }

        return thriftCall(sessionId, timeout, info);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public <T> CompletableFuture<T> thriftCallByInfo(int timeout, InvocationInfo tInfo) throws TException {
        return thriftCall(sessionId, timeout, tInfo);
    }

    @SuppressWarnings("rawtypes")
    protected abstract <T> CompletableFuture<T> thriftCall(S sessionId, int timeout, InvocationInfo tInfo) throws TException;

}
