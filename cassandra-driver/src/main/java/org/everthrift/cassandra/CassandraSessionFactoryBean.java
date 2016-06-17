package org.everthrift.cassandra;

import org.springframework.beans.factory.FactoryBean;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

public class CassandraSessionFactoryBean implements FactoryBean<Session>{

	private Cluster cluster;
	private String keyspace;

	public CassandraSessionFactoryBean() {
		super();
	}

	@Override
	public Session getObject() throws Exception {
		return cluster.connect(keyspace);
	}

	@Override
	public Class<?> getObjectType() {
		return Session.class;
	}

	@Override
	public boolean isSingleton() {
		// TODO Auto-generated method stub
		return false;
	}

	public String getKeyspace() {
		return keyspace;
	}

	public void setKeyspace(String keyspace) {
		this.keyspace = keyspace;
	}	
}
