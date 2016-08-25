package org.everthrift.appserver.controller;

import java.util.Map;

import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TMessage;

public interface ThriftProtocolSupportIF<T> {
    String getSessionId(); // Transport level sessionId

    TMessage getTMessage() throws TException;

    Map<String, Object> getAttributes();

    public <T extends TBase> T readArgs(ThriftControllerInfo tInfo) throws TException;

    void skip() throws TException;

    public T result(final Object o, final ThriftControllerInfo tInfo);

    public void asyncResult(final Object o, final AbstractThriftController controller);

    boolean allowAsyncAnswer();
}