package org.everthrift.clustering.jgroups;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.everthrift.clustering.MessageWrapper;
import org.everthrift.clustering.thrift.InvocationInfo;
import org.jgroups.Address;
import org.jgroups.Message;
import org.jgroups.Message.TransientFlag;
import org.jgroups.blocks.MessageDispatcher;
import org.jgroups.blocks.RequestOptions;
import org.jgroups.blocks.ResponseMode;
import org.jgroups.util.FutureListener;
import org.jgroups.util.Rsp;
import org.jgroups.util.RspList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public abstract class AbstractJgroupsThriftClientImpl extends ClusterThriftClientImpl {

    protected final Logger log = LoggerFactory.getLogger(this.getClass());

    private final TProtocolFactory binaryProtocolFactory = new TBinaryProtocol.Factory();

    public abstract MessageDispatcher getMessageDispatcher();

    protected CompletableFuture<?> viewAccepted = new CompletableFuture();

    public Address getLocalAddress() {
        return getCluster().getAddress();
    }

    @SuppressWarnings("rawtypes")
    private <T> CompletableFuture<Map<Address, Reply<T>>> _thriftCall(Collection<Address> dest, Collection<Address> exclusionList,
                                                                      boolean loopBack, int timeout, ResponseMode responseMode,
                                                                      InvocationInfo tInfo) throws TException {

        final Message msg = new Message();
        msg.setObject(new MessageWrapper(tInfo.buildCall(0, binaryProtocolFactory)));

        if (!loopBack) {
            msg.setTransientFlag(TransientFlag.DONT_LOOPBACK);
        }

        final RequestOptions options = new RequestOptions(responseMode, timeout);

        if (exclusionList != null) {
            options.setExclusionList(exclusionList.toArray(new Address[exclusionList.size()]));
        }

        if (dest != null) {
            options.setAnycasting(true);
        }

        final CompletableFuture<Map<Address, Reply<T>>> f = new CompletableFuture<>();

        log.debug("call {}, dest={}, excl={}, loopback={}, timeout={}, respMode={}", tInfo.fullMethodName, dest, exclusionList, loopBack,
                  timeout, responseMode);

        try {
            getMessageDispatcher().castMessageWithFuture(dest, msg, options, new FutureListener<RspList<MessageWrapper>>() {

                @Override
                public void futureDone(Future<RspList<MessageWrapper>> future) {

                    log.debug("futureDone");

                    RspList<MessageWrapper> resp;
                    try {
                        resp = future.get();
                    } catch (InterruptedException | ExecutionException e1) {
                        f.completeExceptionally(e1);
                        return;
                    }

                    log.trace("RspList:{}", resp);

                    final Map<Address, Reply<T>> ret = new HashMap<>();

                    for (Rsp<MessageWrapper> responce : resp) {
                        if (responce.getValue() != null) {
                            ret.put(responce.getSender(),
                                    new ReplyImpl<T>(() -> (T) tInfo.setReply(responce.getValue()
                                                                                      .getTTransport(), binaryProtocolFactory)));
                        } else {
                            log.warn("null responce from {}", responce.getSender());
                        }
                    }

                    f.complete(ret);
                }
            });
        } catch (Exception e) {
            f.completeExceptionally(e);
        }

        return f;
    }

    @Override
    public <T> CompletableFuture<Map<Address, Reply<T>>> call(Collection<Address> dest, Collection<Address> exclusionList,
                                                              InvocationInfo tInfo, Options... options) throws TException {

        Assert.notNull(tInfo, "tInfo must not be null");

        final CompletableFuture<Map<Address, Reply<T>>> ret = new CompletableFuture<>();

        viewAccepted.whenComplete((result, t) -> {
            if (t == null) {
                try {
                    this.<T>_thriftCall(dest, exclusionList, isLoopback(options), getTimeout(options), getResponseMode(options), tInfo)
                        .whenComplete((result2, t2) -> {
                            if (t2 != null) {
                                ret.completeExceptionally(t2);
                            } else {
                                ret.complete(result2);
                            }
                        });
                } catch (TException e) {
                    ret.completeExceptionally(e);
                }
            }
        });

        return ret;
    }

}
