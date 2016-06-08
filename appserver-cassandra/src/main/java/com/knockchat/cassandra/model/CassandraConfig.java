package com.knockchat.cassandra.model;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class CassandraConfig {
	
	@Bean
	public CassandraFactories cassandraFactories(){
		return new CassandraFactories();
	}
}
