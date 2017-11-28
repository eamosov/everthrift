package org.everthrift.appserver.configs;

import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.everthrift.appserver.cluster.controllers.GetNodeConfigurationController;
import org.everthrift.appserver.cluster.controllers.OnNodeConfigurationController;
import org.everthrift.appserver.controller.ThriftControllerJmx;
import org.everthrift.appserver.controller.ThriftControllerRegistry;
import org.everthrift.appserver.controller.ThriftProcessor;
import org.everthrift.appserver.model.LocalEventBus;
import org.everthrift.utils.tg.AtomicMonotonicTimestampGenerator;
import org.everthrift.utils.tg.TimestampGenerator;
import org.springframework.aop.interceptor.AsyncUncaughtExceptionHandler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.AdviceMode;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.ImportResource;
import org.springframework.context.annotation.Scope;
import org.springframework.scheduling.annotation.AsyncConfigurer;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.SchedulingConfigurer;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.scheduling.config.ScheduledTaskRegistrar;
import org.springframework.transaction.annotation.EnableTransactionManagement;

import java.util.concurrent.Executor;

@Configuration
@ImportResource("classpath:app-context.xml")
@EnableTransactionManagement(mode = AdviceMode.ASPECTJ, proxyTargetClass = true)
@EnableScheduling
@EnableAsync
public class AppserverConfig implements SchedulingConfigurer, AsyncConfigurer {

    public AppserverConfig() {

    }

    @Autowired
    @Qualifier("myScheduler")
    private ThreadPoolTaskScheduler myScheduler;

    @Autowired
    @Qualifier("callerRunsBoundQueueExecutor")
    private ThreadPoolTaskExecutor callerRunsBoundQueueExecutor;

    @Override
    public void configureTasks(ScheduledTaskRegistrar taskRegistrar) {
        taskRegistrar.setTaskScheduler(myScheduler);
    }

    @Override
    public Executor getAsyncExecutor() {
        return callerRunsBoundQueueExecutor;
    }

    @Override
    public AsyncUncaughtExceptionHandler getAsyncUncaughtExceptionHandler() {
        return null;
    }

    @Bean
    public ThreadPoolTaskExecutor callerRunsBoundQueueExecutor() {
        final ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(5);
        executor.setMaxPoolSize(20);
        executor.setKeepAliveSeconds(5);
        executor.setQueueCapacity(200);
        return executor;
    }

    @Bean
    public ThreadPoolTaskExecutor unboundQueueExecutor() {
        final ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(10);
        executor.setMaxPoolSize(10);
        return executor;
    }

    @Bean
    public ThreadPoolTaskScheduler myScheduler() {
        final ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
        scheduler.setPoolSize(10);
        return scheduler;
    }

    @Bean
    public ListeningExecutorService listeningCallerRunsBoundQueueExecutor(@Qualifier("callerRunsBoundQueueExecutor") ThreadPoolTaskExecutor executor) {
        return MoreExecutors.listeningDecorator(executor.getThreadPoolExecutor());
    }

    @Bean
    public ListeningScheduledExecutorService listeningScheduledExecutorService(@Qualifier("myScheduler") ThreadPoolTaskScheduler scheduler) {
        return MoreExecutors.listeningDecorator(scheduler.getScheduledThreadPoolExecutor());
    }

    @Bean
    @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    public ThriftProcessor thriftProcessor(ThriftControllerRegistry registry) {
        return new ThriftProcessor(registry);
    }

    @Bean
    public LocalEventBus LocalEventBus() {
        return new LocalEventBus();
    }

    @Bean
    public ThriftControllerJmx ThriftControllerJmx() {
        return new ThriftControllerJmx();
    }

    @Bean
    @Scope(value = ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    public GetNodeConfigurationController getNodeConfigurationController() {
        return new GetNodeConfigurationController();
    }

    @Bean
    @Scope(value = ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    public OnNodeConfigurationController getOnNodeConfigurationController() {
        return new OnNodeConfigurationController();
    }

    @Bean
    public TimestampGenerator timestampGenerator(){
        return new AtomicMonotonicTimestampGenerator();
    }

    @Bean
    public Boolean testMode(@Value("${app.testMode:false}") String value) {
        return Boolean.parseBoolean(value);
    }
}
