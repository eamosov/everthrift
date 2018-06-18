package org.everthrift.clustering.jgroups;

import org.everthrift.clustering.thrift.ThriftControllerDiscovery;
import org.jgroups.Address;
import org.jgroups.JChannel;
import org.jgroups.MembershipListener;
import org.jgroups.View;
import org.jgroups.blocks.MessageDispatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;

public class JGroupsThriftClientImpl extends AbstractJgroupsThriftClientImpl implements MembershipListener, ClusterThriftClientIF {

    private static final Logger log = LoggerFactory.getLogger(JGroupsThriftClientImpl.class);

    private final JChannel cluster;

    private final String clusterName;

    private MessageDispatcher disp;

    public JGroupsThriftClientImpl(ThriftControllerDiscovery thriftControllerDiscovery, String jgroupsXmlPath, String clusterName) throws Exception {
        this(thriftControllerDiscovery, new JChannel(jgroupsXmlPath), clusterName);
    }

    public JGroupsThriftClientImpl(ThriftControllerDiscovery thriftControllerDiscovery, JChannel cluster, String clusterName) {
        super(thriftControllerDiscovery);
        log.info("Using {} as MulticastThriftTransport", this.getClass().getSimpleName());
        this.cluster = cluster;
        this.clusterName = clusterName;
    }

    public void destroy() {
        cluster.close();
    }

    public void connect() throws Exception {
        log.info("Starting JGroups MessageDispatcher");

        disp = new MessageDispatcher(cluster);
        disp.setMembershipListener(this);
        cluster.connect(clusterName);
    }

    @Override
    public MessageDispatcher getMessageDispatcher() {
        return disp;
    }

    @Override
    public synchronized void viewAccepted(View new_view) {

        if (!this.viewAccepted.isDone()) {
            this.viewAccepted.complete(null);
        }

    }

    @Override
    public synchronized void suspect(Address suspected_mbr) {
    }

    @Override
    public synchronized void block() {

    }

    @Override
    public synchronized void unblock() {

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
