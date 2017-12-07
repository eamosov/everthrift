package org.everthrift.appserver.configs;

import net.sf.ehcache.CacheManager;
import net.sf.ehcache.config.CacheConfiguration;
import net.sf.ehcache.config.ConfigurationFactory;
import org.everthrift.appserver.AppserverApplication;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;
import java.io.InputStream;

/**
 * Created by fluder on 09.03.17.
 */
@Configuration
public class EhConfig {

    @Bean
    public CacheManager ehCache(@NotNull ApplicationContext context,
                                @NotNull @Value("${ehcache.name}") String cacheName,
                                @NotNull @Value("${ehcache.maxBytesLocalHeap}") String maxBytesLocalHeap,
                                @Value("${ehcache.jgroups.multicast.bind_addr}") String bindAddr,
                                @Value("${ehcache.jgroups.udp.mcast_port}") String mcastPort) throws IOException {

        try (InputStream inputStream = context.getResource("/ehcache.xml").getInputStream()) {
            net.sf.ehcache.config.Configuration config = ConfigurationFactory.parseConfiguration(inputStream);

            if (!AppserverApplication.isJGroupsEnabled(context.getEnvironment())) {
                config.getCacheManagerPeerProviderFactoryConfiguration().clear();

                for (CacheConfiguration cc : config.getCacheConfigurations().values()) {
                    cc.getCacheEventListenerConfigurations().clear();
                }

                config.getDefaultCacheConfiguration().getCacheEventListenerConfigurations().clear();
            }

            config.setName(cacheName);
            config.setMaxBytesLocalHeap(maxBytesLocalHeap);
            final CacheManager cm = CacheManager.create(config);
            return cm;
        }
    }

}
