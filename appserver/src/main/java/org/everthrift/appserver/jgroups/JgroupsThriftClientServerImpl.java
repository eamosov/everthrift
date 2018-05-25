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
import org.everthrift.clustering.jgroups.AbstractJgroupsThriftClientImpl;
import org.everthrift.clustering.jgroups.ClusterThriftClientIF;
import org.everthrift.clustering.thrift.ThriftCallFutureHolder;
import org.everthrift.clustering.thrift.ThriftProxyFactory;
import org.everthrift.services.thrift.cluster.ClusterService;
import org.everthrift.utils.ThriftServicesDb;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jgroups.Address;
import org.jgroups.JChannel;
import org.jgroups.MembershipListener;
import org.jgroups.Message;
import org.jgroups.View;
import org.jgroups.blocks.AsyncRequestHandler;
import org.jgroups.blocks.MessageDispatcher;
import org.jgroups.blocks.ResponseMode;
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

    private final RpcJGroupsRegistry rpcJGroupsRegistry;

    private final ThriftProcessor thriftProcessor;

    private final TProtocolFactory protocolFactory = new TBinaryProtocol.Factory();

    @Nullable
    private MessageDispatcher disp;

    @NotNull
    private List<MembershipListener> membershipListeners = new ArrayList<>();

    @Autowired
    private ThriftServicesDb thriftServicesDb;

    private ExecutorService jGroupsExecutorService = Executors.newFixedThreadPool(8, new ThreadFactoryBuilder().setDaemon(true)
                                                                                                               .setNameFormat("JGroups-%d")
                                                                                                               .build());

    public JgroupsThriftClientServerImpl(JChannel cluster, RpcJGroupsRegistry rpcJGroupsRegistry, ThriftProcessor thriftProcessor) {
        log.info("Using {} as MulticastThriftTransport", this.getClass().getSimpleName());
        this.cluster = cluster;
        this.rpcJGroupsRegistry = rpcJGroupsRegistry;
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
            in.setAttribute("src", request.getSrc());

            try {
                final MessageWrapper out = thriftProcessor.process(new DefaultTProtocolSupport(in, protocolFactory) {
                    @Override
                    public void asyncResult(Object o, AbstractThriftController controller) {
                        if (response != null) {
                            response.send(result(o, r ->  controller.getInfo().thriftMethodEntry.makeResult(r)), false);
                        }
                        ThriftProcessor.logEnd(ThriftProcessor.log, controller, msg.name, getSessionId(), o);
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
    public Address getLocalAddress() {
        return cluster.getAddress();
    }

    @Override
    public synchronized void viewAccepted(@NotNull View new_view) {

        if (!this.viewAccepted.isDone()) {
            this.viewAccepted.complete(null);
        }

        nodeDb.retain(new_view.getMembers());
        populateConfiguration();

        for (MembershipListener m : membershipListeners) {
            m.viewAccepted(new_view);
        }
    }

    @Override
    public synchronized void suspect(Address suspected_mbr) {

        populateConfiguration();

        for (MembershipListener m : membershipListeners) {
            m.suspect(suspected_mbr);
        }
    }

    @Override
    public synchronized void block() {

        populateConfiguration();

        for (MembershipListener m : membershipListeners) {
            m.block();
        }
    }

    @Override
    public synchronized void unblock() {

        populateConfiguration();

        for (MembershipListener m : membershipListeners) {
            m.unblock();
        }
    }

    @Scheduled(fixedRate = 5000)
    public void logClusterState() {
        log.info("cluster:{}", cluster.getView());

        populateConfiguration();
    }

    @Override
    public JChannel getCluster() {
        return cluster;
    }

    public void populateConfiguration() {
        try {
            ThriftProxyFactory.on(thriftServicesDb, ClusterService.Iface.class)
                              .onNodeConfiguration(rpcJGroupsRegistry.getNodeConfiguration());
            call(ThriftCallFutureHolder.getThriftCallFuture(), null, Options.loopback(true), Options.responseMode(ResponseMode.GET_NONE));
        } catch (Exception e) {
            log.error("Exception", e);
        }
    }
}
