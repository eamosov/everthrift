package org.everthrift.appserver.controller;

import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TMessage;
import org.everthrift.thrift.TFunction;
import org.jetbrains.annotations.Nullable;

import java.util.Map;
import java.util.function.Function;

public interface ThriftProtocolSupportIF<T> {
    @Nullable
    String getSessionId(); // Transport level sessionId

    TMessage getTMessage() throws TException;

    @Nullable
    Map<String, Object> getAttributes();

    <T extends TBase> T readArgs(TBase args) throws TException;

    void skip() throws TException;

    T result(final Object o, final TFunction<Object, TBase> makeResult);

    void asyncResult(final Object o, final AbstractThriftController controller);

    boolean allowAsyncAnswer();
}