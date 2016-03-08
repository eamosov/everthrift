package com.knockchat.cassandra;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.QueryLogger;
import com.datastax.driver.core.Session;
import com.knockchat.cassandra.codecs.ByteArrayBlobCodec;
import com.knockchat.cassandra.codecs.LongTimestampCodec;
import com.knockchat.cassandra.codecs.StringUuidCodec;

public class CassandraSessionFactoryBean {
	
	private static final Logger log = LoggerFactory.getLogger(CassandraSessionFactoryBean.class);
	
	public static Cluster createCluster(String contactPoint){
		log.info("Connecting to cassandra at {}", contactPoint);
		final Cluster cluster =  Cluster.builder().addContactPoint(contactPoint).build();
		cluster.getConfiguration().getCodecRegistry().register(LongTimestampCodec.instance, StringUuidCodec.instance, ByteArrayBlobCodec.instance);
		
		final QueryLogger queryLogger = QueryLogger.builder().withConstantThreshold(50).withMaxQueryStringLength(-1).build();
		cluster.register(queryLogger);
		return cluster;
	}
	
    public static Session createSession(Cluster cluster, String keyspace){
    	log.info("Connecting claster to keyspace {}", keyspace);
		return cluster.connect(keyspace);
    }
}
