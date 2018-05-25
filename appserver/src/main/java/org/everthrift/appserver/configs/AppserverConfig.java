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
import org.everthrift.thrift.MetaDataMapBuilder;
import org.everthrift.utils.ThriftServicesDb;
import org.everthrift.utils.tg.AtomicMonotonicTimestampGenerator;
import org.everthrift.utils.tg.TimestampGenerator;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

    private static final Logger log = LoggerFactory.getLogger(AppserverConfig.class);

    public AppserverConfig() {

    }

    @Autowired
    @Qualifier("myScheduler")
    private ThreadPoolTaskScheduler myScheduler;

    @Autowired
    @Qualifier("callerRunsBoundQueueExecutor")
    private ThreadPoolTaskExecutor callerRunsBoundQueueExecutor;

    @Override
    public void configureTasks(@NotNull ScheduledTaskRegistrar taskRegistrar) {
        taskRegistrar.setTaskScheduler(myScheduler);
    }

    @Override
    public Executor getAsyncExecutor() {
        return callerRunsBoundQueueExecutor;
    }

    @Nullable
    @Override
    public AsyncUncaughtExceptionHandler getAsyncUncaughtExceptionHandler() {
        return null;
    }

    @NotNull
    @Bean
    public ThreadPoolTaskExecutor callerRunsBoundQueueExecutor() {
        final ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor() {
            @Override
            public Thread createThread(Runnable runnable) {
                final Thread thread = super.createThread(runnable);
                thread.setUncaughtExceptionHandler((t, e) -> {
                    log.error("UncaughtException", e);
                });
                return thread;
            }
        };
        executor.setCorePoolSize(5);
        executor.setMaxPoolSize(20);
        executor.setKeepAliveSeconds(5);
        executor.setQueueCapacity(200);
        return executor;
    }

    @NotNull
    @Bean
    public ThreadPoolTaskExecutor unboundQueueExecutor() {
        final ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor() {
            @Override
            public Thread createThread(Runnable runnable) {
                final Thread thread = super.createThread(runnable);
                thread.setUncaughtExceptionHandler((t, e) -> {
                    log.error("UncaughtException", e);
                });
                return thread;
            }
        };
        executor.setCorePoolSize(10);
        executor.setMaxPoolSize(10);
        return executor;
    }

    @NotNull
    @Bean
    public ThreadPoolTaskScheduler myScheduler() {
        final ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler() {
            @Override
            public Thread createThread(Runnable runnable) {
                final Thread thread = super.createThread(runnable);
                thread.setUncaughtExceptionHandler((t, e) -> {
                    log.error("UncaughtException", e);
                });
                return thread;
            }
        };
        scheduler.setPoolSize(10);
        return scheduler;
    }

    @Bean
    public ListeningExecutorService listeningCallerRunsBoundQueueExecutor(@NotNull @Qualifier("callerRunsBoundQueueExecutor") ThreadPoolTaskExecutor executor) {
        return MoreExecutors.listeningDecorator(executor.getThreadPoolExecutor());
    }

    @Bean
    public ListeningScheduledExecutorService listeningScheduledExecutorService(@NotNull @Qualifier("myScheduler") ThreadPoolTaskScheduler scheduler) {
        return MoreExecutors.listeningDecorator(scheduler.getScheduledThreadPoolExecutor());
    }

    @NotNull
    @Bean
    @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    public ThriftProcessor thriftProcessor(ThriftControllerRegistry registry) {
        return new ThriftProcessor(registry);
    }

    @NotNull
    @Bean
    public LocalEventBus LocalEventBus() {
        return new LocalEventBus();
    }

    @NotNull
    @Bean
    public ThriftControllerJmx ThriftControllerJmx() {
        return new ThriftControllerJmx();
    }

    @NotNull
    @Bean
    @Scope(value = ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    public GetNodeConfigurationController getNodeConfigurationController() {
        return new GetNodeConfigurationController();
    }

    @NotNull
    @Bean
    @Scope(value = ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    public OnNodeConfigurationController getOnNodeConfigurationController() {
        return new OnNodeConfigurationController();
    }

    @NotNull
    @Bean
    public TimestampGenerator timestampGenerator() {
        return new AtomicMonotonicTimestampGenerator();
    }

    @Bean
    public Boolean testMode(@Value("${app.testMode:false}") String value) {
        log.info("Setting testMode: {}", value);
        return Boolean.parseBoolean(value);
    }

    @Bean
    public ThriftServicesDb thriftServicesDb(@Value("${tbase.root}") String tbaseRoot) {

        final MetaDataMapBuilder mdb = new MetaDataMapBuilder();

        for (String root : tbaseRoot.split(",")) {
            mdb.build(root);
        }

        return new ThriftServicesDb(tbaseRoot);
    }
}
