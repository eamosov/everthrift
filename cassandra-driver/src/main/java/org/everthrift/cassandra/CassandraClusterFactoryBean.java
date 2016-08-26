package org.everthrift.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.QueryLogger;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.ThreadLocalMonotonicTimestampGenerator;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.policies.LoggingRetryPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;
import org.everthrift.cassandra.codecs.ByteArrayBlobCodec;
import org.everthrift.cassandra.codecs.LongTimestampCodec;
import org.everthrift.cassandra.codecs.StringUuidCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.util.StringUtils;

import java.util.stream.Stream;

public class CassandraClusterFactoryBean implements FactoryBean<Cluster> {

    private static final Logger log = LoggerFactory.getLogger(CassandraClusterFactoryBean.class);

    private String contactPoints = "locahost";

    private String localDcName;

    private Integer port;

    private String login;

    private String password;

    @Override
    public Cluster getObject() throws Exception {

        Cluster.Builder builder = Cluster.builder();
        Stream.of(contactPoints.split(",")).map(String::trim).forEach(builder::addContactPoint);

        if (port != null) {
            builder.withPort(port);
        }

        DCAwareRoundRobinPolicy.Builder policyBuilder = DCAwareRoundRobinPolicy.builder();
        if (!StringUtils.isEmpty(localDcName)) {
            policyBuilder.withLocalDc(localDcName);
        }
        builder.withLoadBalancingPolicy(new TokenAwarePolicy(policyBuilder.build()));
        builder.withProtocolVersion(ProtocolVersion.V3);
        builder.withRetryPolicy(new LoggingRetryPolicy(DefaultRetryPolicy.INSTANCE));
        builder.withTimestampGenerator(new ThreadLocalMonotonicTimestampGenerator());

        if (login != null || password != null) {
            builder.withCredentials(login, password);
        }

        final Cluster cluster = builder.build();

        final QueryLogger queryLogger = QueryLogger.builder()
                                                   .withConstantThreshold(50)
                                                   .withMaxQueryStringLength(-1)
                                                   .build();
        cluster.register(queryLogger);

        cluster.getConfiguration().getCodecRegistry().register(LongTimestampCodec.instance, StringUuidCodec.instance,
                                                               ByteArrayBlobCodec.instance);
        return cluster;
    }

    public static Session createSession(Cluster cluster, String keyspace) {
        log.info("Connecting claster to keyspace {}", keyspace);
        return cluster.connect(keyspace);
    }

    @Override
    public Class<?> getObjectType() {
        return Cluster.class;
    }

    @Override
    public boolean isSingleton() {
        return true;
    }

    public String getContactPoints() {
        return contactPoints;
    }

    public void setContactPoints(String contactPoints) {
        this.contactPoints = contactPoints;
    }

    public String getLocalDcName() {
        return localDcName;
    }

    public void setLocalDcName(String localDcName) {
        this.localDcName = localDcName;
    }

    public Integer getPort() {
        return port;
    }

    public void setPort(Integer port) {
        this.port = port;
    }

    public String getLogin() {
        return login;
    }

    public void setLogin(String login) {
        this.login = login;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }
}
