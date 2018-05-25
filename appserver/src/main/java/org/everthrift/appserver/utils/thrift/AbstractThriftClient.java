package org.everthrift.appserver.utils.thrift;

import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.everthrift.clustering.thrift.ThriftCallFuture;
import org.everthrift.clustering.thrift.ThriftCallFutureHolder;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;

public abstract class AbstractThriftClient<S> extends ThriftClient<S> {

    public AbstractThriftClient(S sessionId) {
        super(sessionId);
    }

    @Override
    public <T> CompletableFuture<T> thriftCall(int timeout, T methodCall, @NotNull BiConsumer<? super T, ? super Throwable> callback) throws TException {
        final CompletableFuture<T> lf = thriftCall(timeout, methodCall);
        lf.whenComplete(callback);
        return lf;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public <T> CompletableFuture<T> thriftCall(int timeout, T methodCall) throws TException {
        final ThriftCallFuture info = ThriftCallFutureHolder.getThriftCallFuture();
        if (info == null) {
            throw new TTransportException("info is NULL");
        }

        return thriftCall(sessionId, timeout, info);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public <T> CompletableFuture<T> thriftCallByInfo(int timeout, ThriftCallFuture tInfo) throws TException {
        return thriftCall(sessionId, timeout, tInfo);
    }

    @SuppressWarnings("rawtypes")
    protected abstract <T> CompletableFuture<T> thriftCall(S sessionId, int timeout, ThriftCallFuture tInfo) throws TException;

}
