package org.everthrift.appserver.utils.thrift;

import org.apache.thrift.TException;
import org.everthrift.clustering.thrift.InvocationInfo;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ListenableFuture;

public interface ThriftClientIF {

    boolean isThriftCallEnabled();

    <T> ListenableFuture<T> thriftCall(int timeout, T methodCall) throws TException;
    <T> ListenableFuture<T> thriftCall(int timeout, T methodCall, FutureCallback<T> callback) throws TException;
    @SuppressWarnings({ "rawtypes"})
    <T> ListenableFuture<T> thriftCallByInfo(int timeout, InvocationInfo tInfo) throws TException;

    void setSession(SessionIF data);
    SessionIF getSession();

    String getSessionId();
    String getClientIp();
    void addCloseCallback(FutureCallback<Void> callback);
}
