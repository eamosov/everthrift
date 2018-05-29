package org.everthrift.appserver.configs;

import org.everthrift.appserver.controller.ThriftProcessor;
import org.everthrift.appserver.jgroups.JgroupsThriftClientServerImpl;
import org.everthrift.appserver.jgroups.RpcJGroups;
import org.everthrift.clustering.thrift.ThriftControllerDiscovery;
import org.jetbrains.annotations.NotNull;
import org.jgroups.JChannel;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class JGroups {

    @NotNull
    @Bean
    public JChannel yocluster(@NotNull @Value("${jgroups.multicast.bind_addr:127.0.0.1}") String bindAddr,
                              @NotNull @Value("${jgroups.udp.mcast_port:45588}") String mcastPort) throws Exception {

        System.setProperty("jgroups.multicast.bind_addr", bindAddr);
        System.setProperty("jgroups.udp.mcast_port", mcastPort);

        return new JChannel("jgroups.xml");
    }

    @NotNull
    @Bean
    public ThriftProcessor jGroupsThriftProcessor() {
        return new ThriftProcessor(RpcJGroups.class);
    }

    @NotNull
    @Bean
    public JgroupsThriftClientServerImpl jgroupsThriftClientServerImpl(
        ThriftControllerDiscovery thriftControllerDiscovery,
        @Qualifier("yocluster") JChannel cluster,
        @Qualifier("jGroupsThriftProcessor") ThriftProcessor jGroupsThriftProcessor) {

        return new JgroupsThriftClientServerImpl(thriftControllerDiscovery, cluster, jGroupsThriftProcessor);
    }
}
