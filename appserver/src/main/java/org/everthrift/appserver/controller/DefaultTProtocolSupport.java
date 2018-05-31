package org.everthrift.appserver.controller;

import com.google.common.collect.Maps;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TMessage;
import org.apache.thrift.protocol.TMessageType;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolException;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.protocol.TProtocolUtil;
import org.apache.thrift.protocol.TType;
import org.apache.thrift.transport.TMemoryBuffer;
import org.everthrift.clustering.MessageWrapper;
import org.everthrift.thrift.TFunction;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Map;

abstract public class DefaultTProtocolSupport implements ThriftProtocolSupportIF<MessageWrapper> {

    @NotNull
    private final MessageWrapper in;

    private final String sessionId;

    @NotNull
    private final TProtocolFactory protocolFactory;

    private final TProtocol inp;

    protected final TMessage msg;

    private Map<String, Object> attributes;

    public DefaultTProtocolSupport(String sessionId, @NotNull MessageWrapper in, @NotNull TProtocolFactory protocolFactory) throws TException {
        this.in = in;
        this.sessionId = sessionId;
        this.attributes = Maps.newHashMap(in.getAttributes());
        this.protocolFactory = protocolFactory;

        inp = protocolFactory.getProtocol(in.getTTransport());
        msg = inp.readMessageBegin();
    }

    @NotNull
    @Override
    public TMessage getTMessage() throws TException {
        return msg;
    }

    @Nullable
    @Override
    public String getSessionId() {
        return sessionId;
    }

    @NotNull
    @Override
    public <T extends TBase> T deserializeArgs(@NotNull final TBase args) throws TException {
        args.read(inp);
        inp.readMessageEnd();
        return (T) args;
    }

    @Override
    public void skip() throws TException {
        TProtocolUtil.skip(inp, TType.STRUCT);
        inp.readMessageEnd();
    }

    @NotNull
    private MessageWrapper result(@NotNull TApplicationException o) {
        final TMemoryBuffer outT = new TMemoryBuffer(1024);
        final TProtocol out = protocolFactory.getProtocol(outT);
        try {
            out.writeMessageBegin(new TMessage(msg.name, TMessageType.EXCEPTION, msg.seqid));
            ((TApplicationException) o).write(out);
            out.writeMessageEnd();
            out.getTransport().flush(msg.seqid);
        } catch (TException e) {
            throw new RuntimeException(e);
        }
        return new MessageWrapper(outT).putAllAttributes(in.getAttributes());
    }

    @Override
    @NotNull
    public MessageWrapper serializeReply(@Nullable final Object successOrException, final TFunction<Object, TBase> makeResult) {

        if (successOrException instanceof TApplicationException) {
            return result((TApplicationException) successOrException);
        } else if (successOrException instanceof TProtocolException) {
            return result(new TApplicationException(TApplicationException.PROTOCOL_ERROR, ((Exception) successOrException).getMessage()));
        } else if (successOrException instanceof Throwable && !(successOrException instanceof TException)) {
            return result(new TApplicationException(TApplicationException.INTERNAL_ERROR, ((Throwable) successOrException).getMessage()));
        } else {

            final TBase result;
            try {
                result = makeResult.apply(successOrException);
            } catch (TApplicationException e) {
                return result(e);
            } catch (TException e) {
                throw new RuntimeException(e);
            }

            final TMemoryBuffer outT = new TMemoryBuffer(1024);
            final TProtocol out = protocolFactory.getProtocol(outT);

            try {
                out.writeMessageBegin(new TMessage(msg.name, TMessageType.REPLY, msg.seqid));
                result.write(out);
                out.writeMessageEnd();
                out.getTransport().flush(msg.seqid);
            } catch (TException e) {
                throw new RuntimeException(e);
            }

            return new MessageWrapper(outT).putAllAttributes(in.getAttributes());
        }
    }

    @NotNull
    @Override
    public Map<String, Object> getAttributes() {
        return attributes;
    }

    @Override
    public boolean allowAsyncAnswer() {
        return true;
    }
}