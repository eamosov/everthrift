package org.everthrift.appserver.model;

import com.google.common.eventbus.AsyncEventBus;
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.SubscriberExceptionContext;
import com.google.common.eventbus.SubscriberExceptionHandler;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import java.util.concurrent.Executor;

public class LocalEventBus implements InitializingBean {

    private static final Logger log = LoggerFactory.getLogger(LocalEventBus.class);

    @Qualifier("unboundQueueExecutor")
    @Autowired
    private Executor executor;

    private EventBus eventBus;

    private EventBus asyncEventBus;

    @Autowired
    private boolean testMode;

    public LocalEventBus() {

    }

    public LocalEventBus(Executor executor) {
        this.executor = executor;
        afterPropertiesSet();
    }

    @Override
    public void afterPropertiesSet() {
        eventBus = new EventBus(new SubscriberExceptionHandler() {

            @Override
            public void handleException(Throwable exception, SubscriberExceptionContext context) {
                log.error("Exception in eventBus", exception);
            }
        });

        asyncEventBus = new AsyncEventBus(executor, new SubscriberExceptionHandler() {

            @Override
            public void handleException(Throwable exception, SubscriberExceptionContext context) {
                log.error("Exception in asyncEventBus", exception);
            }
        });
    }

    public void register(@NotNull Object object) {
        eventBus.register(object);
        asyncEventBus.register(object);
    }

    public void postEntityEvent(@NotNull Object event) {
        if (testMode) {
            post(event);
        } else {
            postAsync(event);
        }
    }

    public void post(@NotNull Object event) {
        eventBus.post(event);
    }

    public void postAsync(@NotNull Object event) {
        asyncEventBus.post(event);
    }

    public void postAsyncIfNotTest(@NotNull Object event) {
        if (testMode)
            post(event);
        else
            postAsync(event);
    }

    public Executor getExecutor() {
        return executor;
    }

    public void setExecutor(Executor executor) {
        this.executor = executor;
    }

}
