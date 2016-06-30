package org.everthrift.cassandra.model;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration("org.everthrift.cassandra.model.CassandraConfig")
public class CassandraConfig {

    @Bean
    public CassandraFactories cassandraFactories(){
        return new CassandraFactories();
    }
}
