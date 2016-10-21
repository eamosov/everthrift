package org.everthrift.cassandra.scheduling;

import javaslang.control.Option;
import javaslang.control.Try;
import org.everthrift.cassandra.scheduling.context.TriggerContextAccessor;
import org.everthrift.cassandra.scheduling.context.TriggerContextAccessorFactory;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.core.task.TaskRejectedException;
import org.springframework.scheduling.Trigger;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.scheduling.support.TaskUtils;
import org.springframework.util.Assert;
import org.springframework.util.ErrorHandler;

import java.util.Date;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;

public class DistributedScheduledExecutorService implements DistributedTaskScheduler, ApplicationContextAware {

    private volatile ErrorHandler errorHandler;

    private volatile TriggerContextAccessorFactory triggerContextAccessorFactory;

    private volatile ScheduledExecutorService executor;

    private volatile long errorDelay = 10000;

    private ApplicationContext applicationContext;

    private static String DEFAULT_SCHEDULER_NAME = "scheduler";

    public DistributedScheduledExecutorService() {
        super();
    }

    public DistributedScheduledExecutorService(TriggerContextAccessorFactory triggerContextAccessorFactory) {
        super();
        this.triggerContextAccessorFactory = triggerContextAccessorFactory;
    }

    public synchronized void setScheduller(Object o) {
        if (o instanceof ScheduledExecutorService) {
            executor = (ScheduledExecutorService) o;
        } else if (o instanceof ThreadPoolTaskScheduler) {
            executor = ((ThreadPoolTaskScheduler) o).getScheduledExecutor();
        } else {
            throw new IllegalArgumentException("invalid scheduler of type " + o.getClass().getCanonicalName());
        }
    }

    public synchronized ScheduledExecutorService getScheduledExecutor() {
        if (executor == null) {
            executor = Option.of(applicationContext)
                             .flatMap(ac ->
                                          Try.of(() -> ac.getBean(ThreadPoolTaskScheduler.class))
                                             .recover(e -> ac.getBean(DEFAULT_SCHEDULER_NAME, ThreadPoolTaskScheduler.class))
                                             .map(s -> s.getScheduledExecutor())
                                             .getOption()
                             )
                             .getOrElse(() -> Executors.newSingleThreadScheduledExecutor());
        }
        return executor;
    }

    public void setErrorHandler(ErrorHandler errorHandler) {
        this.errorHandler = errorHandler;
    }

    public ScheduledFuture<?> schedule(TriggerContextAccessor ctxh, Runnable task, Trigger trigger) {
        final ScheduledExecutorService executor = getScheduledExecutor();
        try {
            final ErrorHandler errorHandler = (this.errorHandler != null ? this.errorHandler : TaskUtils.getDefaultErrorHandler(true));
            return new DistributedReschedulingRunnable(ctxh, task, trigger, executor, errorHandler, errorDelay).schedule();
        } catch (RejectedExecutionException ex) {
            throw new TaskRejectedException("Executor [" + executor + "] did not accept task: " + task, ex);
        } catch (ContextAccessError e) {
            throw new RuntimeException(e);
        }
    }

    public ScheduledFuture<?> scheduleAtFixedRate(TriggerContextAccessor ctxh, Runnable task, long period) {

        final DistributedPeriodicTrigger trigger = new DistributedPeriodicTrigger(period);
        trigger.setFixedRate(true);
        return schedule(ctxh, task, trigger);
    }

    public ScheduledFuture<?> scheduleAtFixedRate(TriggerContextAccessor ctxh, Runnable task, Date startTime, long period) {
        final DistributedPeriodicTrigger trigger = new DistributedPeriodicTrigger(period);
        trigger.setFixedRate(true);
        trigger.setInitialDelay(startTime.getTime() - System.currentTimeMillis());
        return schedule(ctxh, task, trigger);
    }

    public void setTriggerContextAccessorFactory(TriggerContextAccessorFactory triggerContextAccessorFactory) {
        this.triggerContextAccessorFactory = triggerContextAccessorFactory;
    }

    private TriggerContextAccessorFactory getTriggerContextAccessorFactory() throws IllegalStateException {
        Assert.state(triggerContextAccessorFactory != null, "triggerContextAccessorFactory not initialized");
        return triggerContextAccessorFactory;
    }

    @Override
    public ScheduledFuture<?> schedule(String taskName, Runnable task, Trigger trigger) {
        return schedule(getTriggerContextAccessorFactory().get(taskName), task, trigger);
    }

    @Override
    public ScheduledFuture<?> scheduleAtFixedRate(String taskName, Runnable task, long period) {
        return scheduleAtFixedRate(getTriggerContextAccessorFactory().get(taskName), task, period);
    }

    @Override
    public ScheduledFuture<?> scheduleAtFixedRate(String taskName, Runnable task, Date startTime, long period) {
        return scheduleAtFixedRate(getTriggerContextAccessorFactory().get(taskName), task, startTime, period);
    }

    public ScheduledFuture<?> scheduleWithFixedDelay(TriggerContextAccessor ctxh, Runnable task, Date startTime, long delay) {
        final DistributedPeriodicTrigger trigger = new DistributedPeriodicTrigger(delay);
        trigger.setInitialDelay(startTime.getTime() - System.currentTimeMillis());
        return schedule(ctxh, task, trigger);
    }

    @Override
    public ScheduledFuture<?> scheduleWithFixedDelay(String taskName, Runnable task, Date startTime, long delay) {
        return scheduleWithFixedDelay(getTriggerContextAccessorFactory().get(taskName), task, startTime, delay);
    }

    public ScheduledFuture<?> scheduleWithFixedDelay(TriggerContextAccessor ctxh, Runnable task, long delay) {
        return schedule(ctxh, task, new DistributedPeriodicTrigger(delay));
    }

    @Override
    public ScheduledFuture<?> scheduleWithFixedDelay(String taskName, Runnable task, long delay) {
        return scheduleWithFixedDelay(getTriggerContextAccessorFactory().get(taskName), task, delay);
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }

    public long getErrorDelay() {
        return errorDelay;
    }

    public void setErrorDelay(long errorDelay) {
        this.errorDelay = errorDelay;
    }
}
