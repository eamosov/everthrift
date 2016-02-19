package com.knockchat.appserver.configs;

import org.jgroups.JChannel;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.ImportResource;

import com.knockchat.appserver.transport.jgroups.JGroupsThriftAdapter;
import com.knockchat.appserver.transport.jgroups.JgroupsMessageDispatcher;
import com.knockchat.appserver.transport.jgroups.RpcJGroupsRegistry;

@Configuration
@ImportResource("classpath:jgroups-beans.xml")
public class JGroups {
	
    @Bean
    public JChannel yocluster() throws Exception{
    	return new JChannel("jgroups.xml");
    }
        
    @Bean
    public JgroupsMessageDispatcher jgroupsMessageDispatcher(@Qualifier("yocluster") JChannel cluster){
    	return new JgroupsMessageDispatcher(cluster);
    }

    @Bean
    public RpcJGroupsRegistry RpcJGroupsRegistry(){
    	return new RpcJGroupsRegistry();
    }

    @Bean
    public JGroupsThriftAdapter jGroupsThriftAdapter(){
    	return new JGroupsThriftAdapter();
    }
}
