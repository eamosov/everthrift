package org.everthrift.cassandra;

import org.springframework.beans.factory.FactoryBean;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

public class CassandraSessionFactoryBean implements FactoryBean<Session> {

    private final Cluster cluster;

    private final String keyspace;

    public CassandraSessionFactoryBean(Cluster cluster, String keyspace) {
        super();
        this.cluster = cluster;
        this.keyspace = keyspace;
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
}
