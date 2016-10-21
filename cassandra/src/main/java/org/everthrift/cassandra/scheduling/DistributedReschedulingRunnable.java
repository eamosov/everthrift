package org.everthrift.cassandra.scheduling;

import org.everthrift.cassandra.scheduling.annotation.DistributedScheduled;
import org.everthrift.cassandra.scheduling.context.SettableTriggerContext;
import org.everthrift.cassandra.scheduling.context.TriggerContextAccessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.Trigger;
import org.springframework.scheduling.TriggerContext;
import org.springframework.scheduling.support.DelegatingErrorHandlingRunnable;
import org.springframework.scheduling.support.SimpleTriggerContext;
import org.springframework.util.ErrorHandler;

import java.util.Date;
import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

class DistributedReschedulingRunnable extends DelegatingErrorHandlingRunnable implements ScheduledFuture<Object> {

    private static final Logger log = LoggerFactory.getLogger(DistributedReschedulingRunnable.class);

    private final Trigger trigger;

    private final ScheduledExecutorService executor;

    private ScheduledFuture<?> currentFuture;

    private final TriggerContextAccessor ctxh;

    private final long errorDelay;

    DistributedReschedulingRunnable(TriggerContextAccessor ctxh, Runnable delegate, Trigger trigger, ScheduledExecutorService executor, ErrorHandler errorHandler, long errorDelay) {
        super(delegate, errorHandler);
        this.ctxh = ctxh;
        this.trigger = trigger;
        this.executor = executor;
        this.errorDelay = errorDelay;
    }


    ScheduledFuture<?> schedule() throws ContextAccessError {

        while (true) {
            final SettableTriggerContext ctx = ctxh.get();

            if (ctx.lastScheduledExecutionTime() != null) {
                return schedule(ctx.lastScheduledExecutionTime());
            } else {
                ctx.setLastScheduledExecutionTime(trigger.nextExecutionTime(ctx));

                if (ctx.lastScheduledExecutionTime() == null) {
                    return null;
                }

                if (!ctxh.update(ctx)) {
                    continue;
                }

                return schedule(ctx.lastScheduledExecutionTime());
            }
        }
    }

    @Override
    public void run() {
        try {
            schedule(tryRun());
        } catch (ContextAccessError e) {
            log.error("Error accession context", e);
            schedule(new Date(System.currentTimeMillis() + errorDelay));
        }
    }

    private ScheduledFuture<?> schedule(Date next) {
        if (next == null) {
            return null;
        }

        final long initialDelay = next.getTime() - System.currentTimeMillis();
        synchronized (this) {
            this.currentFuture = this.executor.schedule(this, initialDelay > 0 ? initialDelay : 0, TimeUnit.MILLISECONDS);
        }

        return this;
    }

    private Date tryRun() throws ContextAccessError {
        while (true) {
            final Date now = new Date(System.currentTimeMillis());

            final SettableTriggerContext ctx = ctxh.get();

            if (ctx.lastScheduledExecutionTime() == null) {

                ctx.setLastScheduledExecutionTime(trigger.nextExecutionTime(ctx));

                if (ctx.lastScheduledExecutionTime() == null) {
                    return null;
                }

                if (!ctxh.update(ctx)) {
                    continue;
                }

            } else if (!ctx.lastScheduledExecutionTime().after(now)) {

                final TriggerContext _ctx = new SimpleTriggerContext(ctx.lastScheduledExecutionTime(),
                                                                   ctx.lastActualExecutionTime(),
                                                                   ctx.lastCompletionTime());

                ctx.setLastActualExecutionTime(now);
                ctx.setLastCompletionTime(now);
                ctx.setLastScheduledExecutionTime(trigger.nextExecutionTime(ctx));

                if (!ctxh.update(ctx)) {
                    continue;
                }

                DistributedScheduled.triggerContext.set(_ctx);
                super.run();
                DistributedScheduled.triggerContext.set(null);

                /*
                 * TODO может ли иметь практический смысл запись актуального времени завершения задачи ??
                 * ctxh.updateLastCompletionTime(new Date(System.currentTimeMillis()));
                 */
            }

            return ctx.lastScheduledExecutionTime();
        }
    }

    @Override
    public synchronized boolean cancel(boolean mayInterruptIfRunning) {
        return this.currentFuture.cancel(mayInterruptIfRunning);
    }

    @Override
    public synchronized boolean isCancelled() {
        return this.currentFuture.isCancelled();
    }

    @Override
    public synchronized boolean isDone() {
        return this.currentFuture.isDone();
    }

    @Override
    public Object get() throws InterruptedException, ExecutionException {
        final ScheduledFuture<?> curr;
        synchronized (this) {
            curr = this.currentFuture;
        }
        return curr.get();
    }

    @Override
    public Object get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        final ScheduledFuture<?> curr;
        synchronized (this) {
            curr = this.currentFuture;
        }
        return curr.get(timeout, unit);
    }

    @Override
    public long getDelay(TimeUnit unit) {
        final ScheduledFuture<?> curr;
        synchronized (this) {
            curr = this.currentFuture;
        }
        return curr.getDelay(unit);
    }

    @Override
    public int compareTo(Delayed other) {
        if (this == other) {
            return 0;
        }
        long diff = getDelay(TimeUnit.MILLISECONDS) - other.getDelay(TimeUnit.MILLISECONDS);
        return (diff == 0 ? 0 : ((diff < 0) ? -1 : 1));
    }
}
