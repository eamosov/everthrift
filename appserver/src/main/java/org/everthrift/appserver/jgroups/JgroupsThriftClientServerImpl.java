package org.everthrift.appserver.jgroups;

import org.apache.commons.lang.NotImplementedException;
import org.everthrift.clustering.MessageWrapper;
import org.everthrift.clustering.jgroups.AbstractJgroupsThriftClientImpl;
import org.everthrift.clustering.jgroups.ClusterThriftClientIF;
import org.everthrift.clustering.thrift.InvocationInfoThreadHolder;
import org.everthrift.clustering.thrift.ThriftProxyFactory;
import org.everthrift.services.thrift.cluster.ClusterService;
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
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHeaders;
import org.springframework.scheduling.annotation.Scheduled;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.List;

public class JgroupsThriftClientServerImpl extends AbstractJgroupsThriftClientImpl
    implements AsyncRequestHandler, MembershipListener, ClusterThriftClientIF {

    private static final Logger log = LoggerFactory.getLogger(JgroupsThriftClientServerImpl.class);

    private final JChannel cluster;

    @Autowired
    private ApplicationContext applicationContext;

    @Autowired
    private RpcJGroupsRegistry rpcJGroupsRegistry;

    @Nullable
    private MessageDispatcher disp;

    @NotNull
    private List<MembershipListener> membershipListeners = new ArrayList<MembershipListener>();

    @Resource
    private MessageChannel inJGroupsChannel;

    public JgroupsThriftClientServerImpl(JChannel cluster) {
        log.info("Using {} as MulticastThriftTransport", this.getClass().getSimpleName());
        this.cluster = cluster;
    }

    @NotNull
    @Override
    public Object handle(Message msg) throws Exception {
        log.error("NotImplemented");
        throw new NotImplementedException();
    }

    @Override
    public void handle(@NotNull Message request, @Nullable org.jgroups.blocks.Response response) throws Exception {
        log.debug("handle message: {}, {}", request, response);

        final MessageWrapper w = (MessageWrapper) request.getObject();
        if (response != null) {
            w.setAttribute(JGroupsThriftAdapter.HEADER_JRESPONSE, response);
        }
        w.setAttribute("src", request.getSrc());

        org.springframework.messaging.Message<MessageWrapper> m = MessageBuilder.<MessageWrapper>withPayload(w)
            .setHeader(MessageHeaders.REPLY_CHANNEL,
                       "outJGroupsChannel")
            .build();
        inJGroupsChannel.send(m);
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
            ThriftProxyFactory.on(ClusterService.Iface.class)
                              .onNodeConfiguration(rpcJGroupsRegistry.getNodeConfiguration());
            call(InvocationInfoThreadHolder.getInvocationInfo(), ClusterThriftClientIF.Options.responseMode(ResponseMode.GET_NONE));
        } catch (Exception e) {
            log.error("Exception", e);
        }
    }
}
