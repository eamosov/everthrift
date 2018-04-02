package org.everthrift.appserver.transport.asynctcp;

import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TMemoryBuffer;
import org.apache.thrift.transport.TMemoryInputTransport;
import org.everthrift.appserver.controller.AbstractThriftController;
import org.everthrift.appserver.controller.DefaultTProtocolSupport;
import org.everthrift.appserver.controller.ThriftProcessor;
import org.everthrift.clustering.MessageWrapper;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.support.ChannelInterceptor;
import org.springframework.messaging.support.GenericMessage;

import javax.annotation.Resource;

public class AsyncTcpThriftAdapter implements ChannelInterceptor {

    public static Logger log = LoggerFactory.getLogger(AsyncTcpThriftAdapter.class);

    @Resource
    private MessageChannel outChannel;

    private final ThriftProcessor thriftProcessor;

    private final TProtocolFactory protocolFactory;

    public AsyncTcpThriftAdapter(TProtocolFactory protocolFactory, ThriftProcessor thriftProcessor) {
        this.protocolFactory = protocolFactory;
        this.thriftProcessor = thriftProcessor;
    }

    @Nullable
    public Object handle(@NotNull Message m) {

        log.debug("{}, adapter={}, processor={}", new Object[]{m, this, thriftProcessor});

        try {
            return thriftProcessor.process(new DefaultTProtocolSupport(new MessageWrapper(new TMemoryInputTransport((byte[]) m
                                               .getPayload()))
                                                                           .setMessageHeaders(m.getHeaders()),
                                                                       protocolFactory) {
                                               @Override
                                               public void asyncResult(final Object o, @NotNull final AbstractThriftController controller) {

                                                   final MessageWrapper mw = result(o, controller.getInfo());
                                                   final GenericMessage<MessageWrapper> s = new GenericMessage<MessageWrapper>(mw, m.getHeaders());
                                                   outChannel.send(s);

                                                   ThriftProcessor.logEnd(ThriftProcessor.log, controller, msg.name, null, o);
                                               }
                                           },
                                           null);
        } catch (Exception e) {
            log.error("Exception while execution thrift processor:", e);
            return null;
        }
    }

    @Override
    public Message<?> preSend(@NotNull Message<?> message, MessageChannel channel) {
        return MessageBuilder.withPayload(((TMemoryBuffer) ((MessageWrapper) message.getPayload()).getTTransport()).toByteArray())
                             .copyHeaders(message.getHeaders()).build();
    }

    @Override
    public void postSend(Message<?> message, MessageChannel channel, boolean sent) {

    }

    @Override
    public boolean preReceive(MessageChannel channel) {
        return true;
    }

    @Override
    public Message<?> postReceive(Message<?> message, MessageChannel channel) {
        return message;
    }

    @Override
    public void afterSendCompletion(Message<?> message, MessageChannel channel, boolean sent, Exception ex) {
    }

    @Override
    public void afterReceiveCompletion(Message<?> message, MessageChannel channel, Exception ex) {
    }
}
