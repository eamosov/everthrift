package org.everthrift.cassandra.model;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import org.everthrift.cassandra.CassandraClusterFactoryBean;
import org.everthrift.cassandra.CassandraSessionFactoryBean;
import org.everthrift.cassandra.DbMetadataParser;
import org.everthrift.cassandra.com.datastax.driver.mapping.MappingManager;
import org.everthrift.cassandra.scheduling.DistributedScheduledExecutorService;
import org.everthrift.cassandra.scheduling.DistributedTaskScheduler;
import org.everthrift.cassandra.scheduling.annotation.DistributedScheduledAnnotationBeanPostProcessor;
import org.everthrift.cassandra.scheduling.cassandra.CasTriggerContextAccessorFactory;
import org.everthrift.cassandra.scheduling.context.TriggerContextAccessorFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.support.TaskUtils;

@Configuration("org.everthrift.cassandra.model.CassandraConfig")
public class CassandraConfig {

    @Bean
    public Cluster cluster(
        @Value("${cassandra.contactpoints}") String contactPoints,
        @Value("${cassandra.localDcName:}") String localDcName,
        @Value("${cassandra.port:0}") Integer port,
        @Value("${cassandra.login:}") String login,
        @Value("${cassandra.password:}") String password) throws Exception {

        final CassandraClusterFactoryBean f = new CassandraClusterFactoryBean();

        f.setContactPoints(contactPoints);

        if (!localDcName.isEmpty())
            f.setLocalDcName(localDcName);

        if (port != 0)
            f.setPort(port);

        if (!login.isEmpty())
            f.setLogin(login);

        if (!password.isEmpty())
            f.setPassword(password);

        return f.getObject();
    }

    @Bean
    public Session session(Cluster cluster, @Value("${cassandra.keyspace}") String keyspace) throws Exception {
        final CassandraSessionFactoryBean f = new CassandraSessionFactoryBean(cluster, keyspace);
        return f.getObject();
    }

    @Bean
    public MappingManager mappingManager(Session session) {
        final MappingManager mm = new MappingManager(session, DbMetadataParser.INSTANCE);
        return mm;
    }

    @Bean
    public CassandraFactories cassandraFactories() {
        return new CassandraFactories();
    }

    @Bean
    public static CasTriggerContextAccessorFactory casTriggerContextAccessorFactory(Session session) {
        return new CasTriggerContextAccessorFactory(session);
    }

    @Bean
    public static DistributedScheduledExecutorService distributedScheduledExecutorService(TriggerContextAccessorFactory ctxf) {
        final DistributedScheduledExecutorService s = new DistributedScheduledExecutorService(ctxf);
        s.setErrorHandler(TaskUtils.LOG_AND_SUPPRESS_ERROR_HANDLER);
        return s;
    }

    @Bean
    public static DistributedScheduledAnnotationBeanPostProcessor distributedScheduledAnnotationBeanPostProcessor(DistributedTaskScheduler scheduler) {
        return new DistributedScheduledAnnotationBeanPostProcessor(scheduler);
    }

}
