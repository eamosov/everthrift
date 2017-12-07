package org.everthrift.appserver.scheduling;

import com.google.common.base.Throwables;
import javaslang.control.Option;
import javaslang.control.Try;
import org.everthrift.appserver.model.LocalEventBus;
import org.everthrift.appserver.scheduling.context.SettableTriggerContext;
import org.everthrift.appserver.scheduling.context.TriggerContextAccessor;
import org.everthrift.appserver.scheduling.context.TriggerContextAccessorFactory;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.SmartLifecycle;
import org.springframework.core.task.TaskRejectedException;
import org.springframework.scheduling.Trigger;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.scheduling.support.TaskUtils;
import org.springframework.util.Assert;
import org.springframework.util.ErrorHandler;

import java.io.Serializable;
import java.util.Date;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;

public class DistributedScheduledExecutorService implements DistributedTaskScheduler, ApplicationContextAware, SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(DistributedScheduledExecutorService.class);

    private volatile ErrorHandler errorHandler;

    private volatile TriggerContextAccessorFactory triggerContextAccessorFactory;

    private volatile ScheduledExecutorService executor;

    @Autowired(required = false)
    private LocalEventBus localEventBus;

    private volatile long errorDelay = 10000;

    private ApplicationContext applicationContext;

    @NotNull
    private static String DEFAULT_SCHEDULER_NAME = "scheduler";

    private boolean running = false;

    @Value("${distributed_scheduler.enableDynamicTasks:true}")
    private boolean enableDynamicTasks = true;

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

    @Nullable
    public ScheduledFuture<?> schedule(TriggerContextAccessor ctxh, @NotNull Runnable task, Trigger trigger) {
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

    @Nullable
    public ScheduledFuture<?> scheduleAtFixedRate(TriggerContextAccessor ctxh, @NotNull Runnable task, long period) {

        final DistributedPeriodicTrigger trigger = new DistributedPeriodicTrigger(period);
        trigger.setFixedRate(true);
        return schedule(ctxh, task, trigger);
    }

    @Nullable
    public ScheduledFuture<?> scheduleAtFixedRate(TriggerContextAccessor ctxh, @NotNull Runnable task, @NotNull Date startTime, long period) {
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

    @Nullable
    @Override
    public ScheduledFuture<?> schedule(String taskName, @NotNull Runnable task, Trigger trigger) {
        return schedule(getTriggerContextAccessorFactory().get(taskName, false), task, trigger);
    }

    @Nullable
    @Override
    public ScheduledFuture<?> scheduleAtFixedRate(String taskName, @NotNull Runnable task, long period) {
        return scheduleAtFixedRate(getTriggerContextAccessorFactory().get(taskName, false), task, period);
    }

    @Nullable
    @Override
    public ScheduledFuture<?> scheduleAtFixedRate(String taskName, @NotNull Runnable task, @NotNull Date startTime, long period) {
        return scheduleAtFixedRate(getTriggerContextAccessorFactory().get(taskName, false), task, startTime, period);
    }

    @Nullable
    public ScheduledFuture<?> scheduleWithFixedDelay(TriggerContextAccessor ctxh, @NotNull Runnable task, @NotNull Date startTime, long delay) {
        final DistributedPeriodicTrigger trigger = new DistributedPeriodicTrigger(delay);
        trigger.setInitialDelay(startTime.getTime() - System.currentTimeMillis());
        return schedule(ctxh, task, trigger);
    }

    @Nullable
    @Override
    public ScheduledFuture<?> scheduleWithFixedDelay(String taskName, @NotNull Runnable task, @NotNull Date startTime, long delay) {
        return scheduleWithFixedDelay(getTriggerContextAccessorFactory().get(taskName, false), task, startTime, delay);
    }

    @Nullable
    public ScheduledFuture<?> scheduleWithFixedDelay(TriggerContextAccessor ctxh, @NotNull Runnable task, long delay) {
        return schedule(ctxh, task, new DistributedPeriodicTrigger(delay));
    }

    @Nullable
    @Override
    public ScheduledFuture<?> scheduleWithFixedDelay(String taskName, @NotNull Runnable task, long delay) {
        return scheduleWithFixedDelay(getTriggerContextAccessorFactory().get(taskName, false), task, delay);
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

    /**
     *  TODO в случе какой-то рассинхронизации может получится так, что reScheduleDynamic вызовется несколько раз
     *  для одной и той же задачи. Хотя к ошибкам это и не приведет, нужно устранить такую возможность, т.к. она может
     *  создать лишний контеншн
    */
    @Nullable
    public ScheduledFuture reScheduleDynamic(final String taskName) {
        final TriggerContextAccessor ctxh = getTriggerContextAccessorFactory().get(taskName, true);

        final SettableTriggerContext tx;
        try {
            tx = ctxh.get();
        } catch (ContextAccessError contextAccessError) {
            log.error("Coudn't read task context, id=" + taskName, contextAccessError);
            return null;
        }

        if (tx == null) {
            log.warn("No task context, id={}", taskName);
            return null;
        }

        if (!tx.isDynamic()) {
            log.error("Task {} is not dynamic", taskName);
            return null;
        }

        if (tx.isCancelled()) {
            log.warn("Task {} is cancelled", taskName);
            return null;
        }

        final DynamicTaskRunnable r;

        try {
            r = applicationContext.getBean(tx.getBeanName(), DynamicTaskRunnable.class);
        } catch (BeansException e) {
            log.debug("No bean of name {}", tx.getBeanName());
            return null;
        }

        return schedule(ctxh, () -> r.runTask(taskName, tx.getArg()), new DistributedPeriodicDynamicTrigger());
    }

    @Nullable
    @Override
    public ScheduledFuture<?> scheduleAtFixedRate(String taskName, String beanName, Serializable arg, @NotNull Date startTime, long period) throws DuplicatedTaskException {
        getTriggerContextAccessorFactory().createDynamic(taskName, period, startTime.getTime(), beanName, arg);

        if (localEventBus != null) {
            localEventBus.postAsync(new DynamicTaskEvent(taskName));
        }

        return reScheduleDynamic(taskName);
    }

    @Override
    public void cancel(String taskName) {
        final TriggerContextAccessor accessor = getTriggerContextAccessorFactory().get(taskName, true);
        SettableTriggerContext ctx;

        try {
            do {
                ctx = accessor.get();
                if (ctx == null) {
                    log.warn("Task '{}' not found", taskName);
                    return;
                }

                if (!ctx.isDynamic()) {
                    log.warn("Cancelling not dynamic task {}", taskName);
                }

                if (ctx.isCancelled()) {
                    return;
                }

                ctx.setCancelled(true);
            } while (!accessor.update(ctx));

        } catch (ContextAccessError e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public void update(String taskName, Date startTime, long period){
        final TriggerContextAccessor accessor = getTriggerContextAccessorFactory().get(taskName, true);
        SettableTriggerContext ctx;

        try {
            do {
                ctx = accessor.get();
                if (ctx == null) {
                    log.warn("Task '{}' not found", taskName);
                    return;
                }

                if (!ctx.isDynamic()) {
                    log.warn("Updating not dynamic task {}", taskName);
                }

                ctx.setLastScheduledExecutionTime(startTime);
                ctx.setPeriod(period);
            } while (!accessor.update(ctx));

        } catch (ContextAccessError e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public boolean isAutoStartup() {
        return true;
    }

    @Override
    public void stop(@NotNull Runnable callback) {
        stop();
        callback.run();
    }

    @Override
    public void start() {
        running = true;
        if (enableDynamicTasks) {
            getTriggerContextAccessorFactory().getAllDynamic().forEach(this::reScheduleDynamic);
        }
    }

    @Override
    public void stop() {
        running = false;
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public int getPhase() {
        return 0;
    }

    public LocalEventBus getLocalEventBus() {
        return localEventBus;
    }

    public void setLocalEventBus(LocalEventBus localEventBus) {
        this.localEventBus = localEventBus;
    }

    @Override
    public void onDynamicTaskEvent(@NotNull DynamicTaskEvent e) {
        reScheduleDynamic(e.getTaskName());
    }
}
