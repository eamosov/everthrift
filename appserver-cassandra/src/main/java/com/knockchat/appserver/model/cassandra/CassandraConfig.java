package com.knockchat.appserver.model.cassandra;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class CassandraConfig {
	
	@Bean
	public CassandraFactories cassandraFactories(){
		return new CassandraFactories();
	}
}
