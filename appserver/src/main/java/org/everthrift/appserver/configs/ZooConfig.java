package org.everthrift.appserver.configs;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Created by fluder on 09.03.17.
 */
@Configuration
public class ZooConfig {

    private static final Logger log = LoggerFactory.getLogger(ZooConfig.class);

    @Bean
    public CuratorFramework curatorFramework(final @Value("${zookeeper.connection_string}") String zookeeperConnectionString) {

        log.info("Starting bean: CuratorFramework(\"{}\")", zookeeperConnectionString);

        final RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        final CuratorFramework client = CuratorFrameworkFactory.newClient(zookeeperConnectionString, retryPolicy);
        client.start();
        return client;
    }

}
