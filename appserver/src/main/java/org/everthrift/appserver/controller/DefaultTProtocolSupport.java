package org.everthrift.appserver.controller;

import java.util.Map;

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
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.support.GenericMessage;

public class DefaultTProtocolSupport implements ThriftProtocolSupportIF<MessageWrapper>{

    private final MessageWrapper in;
    private final TProtocolFactory protocolFactory;
    private final TProtocol inp;
    private final TMessage msg;

    public DefaultTProtocolSupport(MessageWrapper in, TProtocolFactory protocolFactory) throws TException{
        this.in = in;
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

    @Override
    public <T extends TBase> T readArgs(final ThriftControllerInfo tInfo) throws TException{
        final TBase args = tInfo.makeArgument();
        args.read(inp);
        inp.readMessageEnd();
        return (T)args;
    }

    @Override
    public void skip() throws TException {
        TProtocolUtil.skip( inp, TType.STRUCT );
        inp.readMessageEnd();
    }

    private MessageWrapper result(TApplicationException o){
        final TMemoryBuffer outT = new TMemoryBuffer(1024);
        final TProtocol out = protocolFactory.getProtocol(outT);
        try{
            out.writeMessageBegin( new TMessage( msg.name, TMessageType.EXCEPTION, msg.seqid));
            ((TApplicationException)o).write(out);
            out.writeMessageEnd();
            out.getTransport().flush(msg.seqid);
        }catch (TException e){
            throw new RuntimeException(e);
        }
        return new MessageWrapper(outT).copyAttributes(in).removeCorrelationHeaders();
    }

    @Override
    public MessageWrapper result(final Object o, final ThriftControllerInfo tInfo) {

        if (o instanceof TApplicationException){
            return result((TApplicationException)o);
        }else if (o instanceof TProtocolException) {
            return result(new TApplicationException(TApplicationException.PROTOCOL_ERROR, ((Exception)o).getMessage()));
        }else if (o instanceof Exception && !(o instanceof TException)){
            return result(new TApplicationException(TApplicationException.INTERNAL_ERROR, ((Exception)o).getMessage()));
        }else{
            final TBase result = tInfo.makeResult(o);
            final TMemoryBuffer outT = new TMemoryBuffer(1024);
            final TProtocol out = protocolFactory.getProtocol(outT);

            try{
                out.writeMessageBegin( new TMessage( msg.name, TMessageType.REPLY, msg.seqid) );
                result.write(out);
                out.writeMessageEnd();
                out.getTransport().flush(msg.seqid);
            }catch (TException e){
                throw new RuntimeException(e);
            }

            return new MessageWrapper(outT).copyAttributes(in).removeCorrelationHeaders();
        }
    }

    @Override
    public void asyncResult(final Object o, final AbstractThriftController controller) {


        final MessageWrapper mw = result(o, controller.getInfo());

        final MessageChannel outChannel = in.getOutChannel();
        final MessageHeaders inHeaders = in.getMessageHeaders();

        final GenericMessage<MessageWrapper> s = new GenericMessage<MessageWrapper>(mw, inHeaders);
        outChannel.send(s);

        ThriftProcessor.logEnd(ThriftProcessor.log, controller, msg.name, in.getSessionId(), o);
    }

    @Override
    public Map<String, Object> getAttributes() {
        return in.getAttributes();
    }

    @Override
    public boolean allowAsyncAnswer() {
        return true;
    }
}