package org.everthrift.appserver.configs;

import org.apache.curator.framework.CuratorFramework;
import org.everthrift.appserver.utils.zooprops.ZPersistanceMbeanExporter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jmx.support.RegistrationPolicy;

import javax.management.MBeanServer;

/**
 * Created by fluder on 09.03.17.
 */
@Configuration
public class ZooJmxConfig {

    @Bean
    public ZPersistanceMbeanExporter persistanceMbeanExporter(CuratorFramework curator,
                                                              ApplicationContext context,
                                                              @Value("${zookeeper.rootPath}") String rootPath,
                                                              MBeanServer mbeanServer) {
        final ZPersistanceMbeanExporter e = new ZPersistanceMbeanExporter(curator, context, rootPath);
        e.setServer(mbeanServer);
        e.setRegistrationPolicy(RegistrationPolicy.REPLACE_EXISTING);
        e.addExcludedBean("jmsContainerManager");
        return e;
    }

}
