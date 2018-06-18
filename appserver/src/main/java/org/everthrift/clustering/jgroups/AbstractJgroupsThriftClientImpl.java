package org.everthrift.clustering.jgroups;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.everthrift.clustering.MessageWrapper;
import org.everthrift.clustering.thrift.ThriftControllerDiscovery;
import org.everthrift.thrift.ThriftCallFuture;
import org.jgroups.Address;
import org.jgroups.Message.TransientFlag;
import org.jgroups.blocks.MessageDispatcher;
import org.jgroups.blocks.RequestOptions;
import org.jgroups.blocks.ResponseMode;
import org.jgroups.util.Buffer;
import org.jgroups.util.Rsp;
import org.jgroups.util.RspList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.utils.SerializationUtils;
import org.springframework.util.Assert;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public abstract class AbstractJgroupsThriftClientImpl extends ClusterThriftClientImpl {

    protected final Logger log = LoggerFactory.getLogger(this.getClass());

    private final TProtocolFactory binaryProtocolFactory = new TBinaryProtocol.Factory();

    public abstract MessageDispatcher getMessageDispatcher();

    protected CompletableFuture<?> viewAccepted = new CompletableFuture();

    public AbstractJgroupsThriftClientImpl(ThriftControllerDiscovery thriftControllerDiscovery) {
        super(thriftControllerDiscovery);
    }

    @SuppressWarnings("rawtypes")
    private <T> CompletableFuture<Map<Address, Reply<T>>> _thriftCall(Collection<Address> dest, Collection<Address> exclusionList,
                                                                      boolean loopBack, int timeout, ResponseMode responseMode,
                                                                      ThriftCallFuture<T> tInfo,
                                                                      Map<String, Object> attributes) throws TException {

        //final Message msg = new Message();
        final MessageWrapper wrap = new MessageWrapper(tInfo.serializeCall(0, binaryProtocolFactory));
        if (attributes != null) {
            wrap.putAllAttributes(attributes);
        }

        final byte[] bytes = SerializationUtils.serialize(wrap);

        final RequestOptions options = new RequestOptions(responseMode, timeout);

        if (dest != null) {
            loopBack = true;
        } else if (!loopBack) {
            options.transientFlags(TransientFlag.DONT_LOOPBACK);
        }

        if (exclusionList != null) {
            options.exclusionList(exclusionList.toArray(new Address[exclusionList.size()]));
        }

        if (dest != null) {
            options.setAnycasting(true);
        }

        log.debug("call {}, dest={}, excl={}, loopback={}, timeout={}, respMode={}", tInfo.getFullMethodName(), dest, exclusionList, loopBack,
                  timeout, responseMode);

        final CompletableFuture<RspList<MessageWrapper>> ret;
        try {
            ret = getMessageDispatcher().castMessageWithFuture(dest, new Buffer(bytes), options);
        } catch (Exception e) {
            throw new TException(e.getMessage(), e);
        }

        return ret.thenApply(rspList -> {
            log.trace("RspList:{}", rspList);

            final Map<Address, Reply<T>> ret1 = new HashMap<>();

            for (Map.Entry<Address, Rsp<MessageWrapper>> response : rspList.entrySet()) {
                final Address sender = response.getKey();
                final Rsp<MessageWrapper> rsp = response.getValue();

                if (rsp.getValue() != null) {
                    ret1.put(sender, new ReplyImpl<T>(() -> tInfo.deserializeReply(rsp.getValue()
                                                                                      .getTTransport(), binaryProtocolFactory)));
                } else {
                    log.warn("null response from {}", sender);
                }
            }
            return ret1;
        });

    }

    @Override
    public <T> CompletableFuture<Map<Address, Reply<T>>> call(Collection<Address> dest,
                                                              Collection<Address> exclusionList,
                                                              ThriftCallFuture<T> tInfo,
                                                              Map<String, Object> attributes,
                                                              Options... options) throws TException {

        Assert.notNull(tInfo, "tInfo must not be null");

        final CompletableFuture<Map<Address, Reply<T>>> ret = new CompletableFuture<>();

        viewAccepted.whenComplete((result, t) -> {
            if (t == null) {
                try {
                    this.<T>_thriftCall(dest, exclusionList, isLoopback(options), getTimeout(options), getResponseMode(options), tInfo, attributes)
                        .whenComplete((result2, t2) -> {
                            if (t2 != null) {
                                ret.completeExceptionally(t2);
                            } else {
                                ret.complete(result2);
                            }
                        });
                } catch (Throwable e) {
                    ret.completeExceptionally(e);
                }
            }
        });

        return ret;
    }

}
