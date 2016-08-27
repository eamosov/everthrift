package org.everthrift.appserver.utils.thrift;

import com.google.common.util.concurrent.FutureCallback;
import org.apache.thrift.TException;
import org.everthrift.clustering.thrift.InvocationInfo;

import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;

public interface ThriftClientIF {

    boolean isThriftCallEnabled();

    <T> CompletableFuture<T> thriftCall(int timeout, T methodCall) throws TException;

    <T> CompletableFuture<T> thriftCall(int timeout, T methodCall, BiConsumer<? super T, ? super Throwable> callback) throws TException;

    @SuppressWarnings({"rawtypes"})
    <T> CompletableFuture<T> thriftCallByInfo(int timeout, InvocationInfo tInfo) throws TException;

    void setSession(SessionIF data);

    SessionIF getSession();

    String getSessionId();

    String getClientIp();

    void addCloseCallback(FutureCallback<Void> callback);
}
