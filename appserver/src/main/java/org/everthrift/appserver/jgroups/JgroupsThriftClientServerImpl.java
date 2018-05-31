package org.everthrift.appserver.jgroups;

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.lang.NotImplementedException;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.everthrift.appserver.controller.AbstractThriftController;
import org.everthrift.appserver.controller.DefaultTProtocolSupport;
import org.everthrift.appserver.controller.ThriftProcessor;
import org.everthrift.clustering.MessageWrapper;
import org.everthrift.clustering.thrift.ThriftControllerDiscovery;
import org.everthrift.clustering.jgroups.AbstractJgroupsThriftClientImpl;
import org.everthrift.clustering.jgroups.ClusterThriftClientIF;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jgroups.Address;
import org.jgroups.JChannel;
import org.jgroups.MembershipListener;
import org.jgroups.Message;
import org.jgroups.View;
import org.jgroups.blocks.AsyncRequestHandler;
import org.jgroups.blocks.MessageDispatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.scheduling.annotation.Scheduled;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class JgroupsThriftClientServerImpl extends AbstractJgroupsThriftClientImpl
    implements AsyncRequestHandler, MembershipListener, ClusterThriftClientIF {

    private static final Logger log = LoggerFactory.getLogger(JgroupsThriftClientServerImpl.class);

    private final JChannel cluster;

    @Autowired
    private ApplicationContext applicationContext;

    private final ThriftProcessor thriftProcessor;

    private final TProtocolFactory protocolFactory = new TBinaryProtocol.Factory();

    @Nullable
    private MessageDispatcher disp;

    @NotNull
    private List<MembershipListener> membershipListeners = new ArrayList<>();

    private ExecutorService jGroupsExecutorService = Executors.newFixedThreadPool(8, new ThreadFactoryBuilder().setDaemon(true)
                                                                                                               .setNameFormat("JGroups-%d")
                                                                                                               .build());

    public JgroupsThriftClientServerImpl(ThriftControllerDiscovery thriftControllerDiscovery, JChannel cluster, ThriftProcessor thriftProcessor) {
        super(thriftControllerDiscovery);
        log.info("Using {} as MulticastThriftTransport", this.getClass().getSimpleName());
        this.cluster = cluster;
        this.thriftProcessor = thriftProcessor;
    }

    @NotNull
    @Override
    public Object handle(Message msg) throws Exception {
        log.error("NotImplemented");
        throw new NotImplementedException();
    }

    @Override
    public void handle(@NotNull Message request, @Nullable org.jgroups.blocks.Response response) throws Exception {

        jGroupsExecutorService.submit(() -> {

            log.debug("handle message: {}, {}", request, response);

            final MessageWrapper in = (MessageWrapper) request.getObject();

            try {
                final MessageWrapper out = thriftProcessor.process(new DefaultTProtocolSupport(null, in, protocolFactory) {
                    @Override
                    public void serializeReplyAsync(Object successOrException, @NotNull AbstractThriftController controller) {
                        if (response != null) {
                            response.send(serializeReply(successOrException, r -> controller.getThriftMethodEntry().makeResult(r)), false);
                        }
                        ThriftProcessor.logEnd(ThriftProcessor.log, controller, msg.name, getSessionId(), successOrException);
                    }
                }, null);

                if (out != null && response != null) {
                    response.send(out, false);
                }

            } catch (TException e) {
                throw Throwables.propagate(e);
            }
        });
    }

    @PreDestroy
    public void destroy() {
        cluster.close();
    }

    public synchronized void addMembershipListener(MembershipListener m) {
        membershipListeners.add(m);
    }

    public synchronized void removeMembershipListener(MembershipListener m) {
        membershipListeners.remove(m);
    }

    @PostConstruct
    public void connect() throws Exception {
        log.info("Starting JgroupsMessageDispatcher");

        disp = new MessageDispatcher(cluster, null, this, this);
        disp.asyncDispatching(true);

        cluster.connect(applicationContext.getEnvironment().getProperty("jgroups.cluster.name"));
    }

    @Nullable
    @Override
    public MessageDispatcher getMessageDispatcher() {
        return disp;
    }

    @Override
    public synchronized void viewAccepted(@NotNull View new_view) {

        if (!this.viewAccepted.isDone()) {
            this.viewAccepted.complete(null);
        }

        for (MembershipListener m : membershipListeners) {
            m.viewAccepted(new_view);
        }
    }

    @Override
    public synchronized void suspect(Address suspected_mbr) {
        for (MembershipListener m : membershipListeners) {
            m.suspect(suspected_mbr);
        }
    }

    @Override
    public synchronized void block() {
        for (MembershipListener m : membershipListeners) {
            m.block();
        }
    }

    @Override
    public synchronized void unblock() {
        for (MembershipListener m : membershipListeners) {
            m.unblock();
        }
    }

    @Scheduled(fixedRate = 5000)
    public void logClusterState() {
        log.info("cluster:{}", cluster.getView());
    }

    @Override
    public JChannel getCluster() {
        return cluster;
    }
}
