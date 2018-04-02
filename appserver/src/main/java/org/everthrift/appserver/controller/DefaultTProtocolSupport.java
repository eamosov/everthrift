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
import org.jetbrains.annotations.NotNull;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.support.GenericMessage;

import java.util.Map;

abstract public class DefaultTProtocolSupport implements ThriftProtocolSupportIF<MessageWrapper> {

    @NotNull
    private final MessageWrapper in;

    @NotNull
    private final TProtocolFactory protocolFactory;

    private final TProtocol inp;

    protected final TMessage msg;

    private Map<String, Object> attributes;

    public DefaultTProtocolSupport(@NotNull MessageWrapper in, @NotNull TProtocolFactory protocolFactory) throws TException {
        this.in = in;
        this.attributes = Maps.newHashMap(in.getAttributes());
        this.protocolFactory = protocolFactory;

        inp = protocolFactory.getProtocol(in.getTTransport());
        msg = inp.readMessageBegin();
    }

    @Override
    public String getSessionId() {
        return in.getSessionId();
    }

    @Override
    public TMessage getTMessage() throws TException {
        return msg;
    }

    @NotNull
    @Override
    public <T extends TBase> T readArgs(@NotNull final ThriftControllerInfo tInfo) throws TException {
        final TBase args = tInfo.makeArgument();
        args.read(inp);
        inp.readMessageEnd();
        return (T) args;
    }

    @Override
    public void skip() throws TException {
        TProtocolUtil.skip(inp, TType.STRUCT);
        inp.readMessageEnd();
    }

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
        return new MessageWrapper(outT).copyAttributes(in).removeCorrelationHeaders();
    }

    @Override
    public MessageWrapper result(final Object o, @NotNull final ThriftControllerInfo tInfo) {

        if (o instanceof TApplicationException) {
            return result((TApplicationException) o);
        } else if (o instanceof TProtocolException) {
            return result(new TApplicationException(TApplicationException.PROTOCOL_ERROR, ((Exception) o).getMessage()));
        } else if (o instanceof Throwable && !(o instanceof TException)) {
            return result(new TApplicationException(TApplicationException.INTERNAL_ERROR, ((Throwable) o).getMessage()));
        } else {

            final TBase result;
            try {
                result = tInfo.makeResult(o);
            } catch (TApplicationException e) {
                return result(e);
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

            return new MessageWrapper(outT).copyAttributes(in).removeCorrelationHeaders();
        }
    }

//    @Override
//    public void asyncResult(final Object o, @NotNull final AbstractThriftController controller) {
//
//        final MessageWrapper mw = result(o, controller.getInfo());
//
//        final MessageChannel outChannel = in.getOutChannel();
//        final MessageHeaders inHeaders = in.getMessageHeaders();
//
//        final GenericMessage<MessageWrapper> s = new GenericMessage<MessageWrapper>(mw, inHeaders);
//        outChannel.send(s);
//
//        ThriftProcessor.logEnd(ThriftProcessor.log, controller, msg.name, in.getSessionId(), o);
//    }

    @Override
    public Map<String, Object> getAttributes() {
        return attributes;
    }

    @Override
    public boolean allowAsyncAnswer() {
        return true;
    }
}