package org.everthrift.appserver.controller;

import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TMessage;
import org.everthrift.thrift.TFunction;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Map;
import java.util.function.Function;

public interface ThriftProtocolSupportIF<T> {
    @Nullable
    String getSessionId(); // Transport level sessionId

    @NotNull
    TMessage getTMessage() throws TException;

    @NotNull
    Map<String, Object> getAttributes();

    @NotNull
    <T extends TBase> T deserializeArgs(TBase args) throws TException;

    void skip() throws TException;

    @NotNull
    T serializeReply(@Nullable final Object successOrException, final TFunction<Object, TBase> makeResult);

    void serializeReplyAsync(@Nullable final Object successOrException, @NotNull final AbstractThriftController controller);

    boolean allowAsyncAnswer();
}