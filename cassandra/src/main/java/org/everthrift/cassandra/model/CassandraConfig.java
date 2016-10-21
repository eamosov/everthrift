package org.everthrift.cassandra.model;

import com.datastax.driver.core.Session;
import org.everthrift.cassandra.scheduling.DistributedScheduledExecutorService;
import org.everthrift.cassandra.scheduling.DistributedTaskScheduler;
import org.everthrift.cassandra.scheduling.annotation.DistributedScheduledAnnotationBeanPostProcessor;
import org.everthrift.cassandra.scheduling.cassandra.CasTriggerContextAccessorFactory;
import org.everthrift.cassandra.scheduling.context.TriggerContextAccessorFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.support.TaskUtils;

@Configuration("org.everthrift.cassandra.model.CassandraConfig")
public class CassandraConfig {

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
