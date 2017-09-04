package org.everthrift.appserver.configs;

import org.everthrift.appserver.scheduling.DistributedScheduledExecutorService;
import org.everthrift.appserver.scheduling.DistributedTaskScheduler;
import org.everthrift.appserver.scheduling.annotation.DistributedScheduledAnnotationBeanPostProcessor;
import org.everthrift.appserver.scheduling.context.TriggerContextAccessorFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.support.TaskUtils;

/**
 * Created by fluder on 01/09/17.
 */
@Configuration
@ConditionalOnBean(TriggerContextAccessorFactory.class)
public class DistributedSchedulerConfig {

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
