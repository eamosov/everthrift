package org.everthrift.appserver.configs;

import com.google.common.collect.ImmutableList;
import org.everthrift.appserver.jgroups.JGroupsThriftAdapter;
import org.everthrift.appserver.jgroups.JgroupsThriftClientServerImpl;
import org.everthrift.appserver.jgroups.RpcJGroupsRegistry;
import org.jetbrains.annotations.NotNull;
import org.jgroups.JChannel;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.ImportResource;

import java.util.List;

@Configuration
@ImportResource("classpath:jgroups-beans.xml")
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
    public JgroupsThriftClientServerImpl jgroupsThriftClientServerImpl(@Qualifier("yocluster") JChannel cluster) {
        return new JgroupsThriftClientServerImpl(cluster);
    }

    @NotNull
    @Bean
    public RpcJGroupsRegistry RpcJGroupsRegistry() {
        return new RpcJGroupsRegistry();
    }

    @NotNull
    @Bean
    public JGroupsThriftAdapter jGroupsThriftAdapter() {
        return new JGroupsThriftAdapter();
    }
}
