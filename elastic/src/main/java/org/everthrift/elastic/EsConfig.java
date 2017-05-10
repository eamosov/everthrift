package org.everthrift.elastic;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.everthrift.elastic.pilato.ElasticsearchAbstractClientFactoryBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.net.InetAddress;

/**
 * Created by fluder on 09.03.17.
 */
@Configuration
public class EsConfig {

    private static final Logger log = LoggerFactory.getLogger(EsConfig.class);

    @Bean
    public Client esClient(@Value("${es.cluster.name}") String clusterName,
                           @Value("${es.network.host}") String transportAddress,
                           @Value("${es.discovery}") String discovery,
                           @Value("${es.index.prefix}") String indexPrefix) throws Exception {

        log.info("es.network.host={}", transportAddress);
        log.info("discovery.zen.ping.unicast.hosts={}", discovery);

        final Settings settings = Settings.builder()
                                          .put("cluster.name", clusterName)
                                          .build();


        final TransportClient _client = new PreBuiltTransportClient(settings);
        for (String address : discovery.split(",;")) {
            //TODO выделить из адреса порт, если есть
            _client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(address), 9300));
        }

        final ElasticsearchAbstractClientFactoryBean esHelper = new ElasticsearchAbstractClientFactoryBean() {

            @Override
            protected Client buildClient() throws Exception {
                return _client;
            }
        };

        esHelper.setIndexPrefix(indexPrefix);
        esHelper.setAutoscan(true);
        esHelper.afterPropertiesSet();

        return _client;
    }

}
