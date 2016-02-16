package com.knockchat.cassandra.migrator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

public class CassandraSessionFactoryBean {
	
	private static final Logger log = LoggerFactory.getLogger(CassandraSessionFactoryBean.class);
	
	public static Cluster createCluster(String contactPoint){
		log.info("Connecting to cassandra at {}", contactPoint);
		return Cluster.builder().addContactPoint(contactPoint).build();		
	}
	
    public static Session createSession(Cluster cluster, String keyspace){
    	log.info("Connecting claster to keyspace {}", keyspace);
		return cluster.connect(keyspace);
    }
}
