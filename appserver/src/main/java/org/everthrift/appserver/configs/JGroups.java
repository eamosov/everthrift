package org.everthrift.appserver.configs;

import org.everthrift.appserver.jgroups.JGroupsThriftAdapter;
import org.everthrift.appserver.jgroups.JgroupsThriftClientServerImpl;
import org.everthrift.appserver.jgroups.RpcJGroupsRegistry;
import org.jgroups.JChannel;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.ImportResource;

@Configuration
@ImportResource("classpath:jgroups-beans.xml")
public class JGroups {

    @Bean
    public JChannel yocluster() throws Exception {
        return new JChannel("jgroups.xml");
    }

    @Bean
    public JgroupsThriftClientServerImpl jgroupsThriftClientServerImpl(@Qualifier("yocluster") JChannel cluster) {
        return new JgroupsThriftClientServerImpl(cluster);
    }

    @Bean
    public RpcJGroupsRegistry RpcJGroupsRegistry() {
        return new RpcJGroupsRegistry();
    }

    @Bean
    public JGroupsThriftAdapter jGroupsThriftAdapter() {
        return new JGroupsThriftAdapter();
    }
}
