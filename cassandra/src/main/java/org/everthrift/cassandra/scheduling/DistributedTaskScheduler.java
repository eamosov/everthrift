package org.everthrift.cassandra.scheduling;

import org.springframework.scheduling.Trigger;

import java.io.Serializable;
import java.util.Date;
import java.util.concurrent.ScheduledFuture;

public interface DistributedTaskScheduler {

    ScheduledFuture<?> schedule(String taskName, Runnable task, Trigger trigger);

    ScheduledFuture<?> scheduleAtFixedRate(String taskName, Runnable task, long period);

    ScheduledFuture<?> scheduleAtFixedRate(String taskName, Runnable task, Date startTime, long period);

    ScheduledFuture<?> scheduleWithFixedDelay(String taskName, Runnable task, Date startTime, long delay);

    ScheduledFuture<?> scheduleWithFixedDelay(String taskName, Runnable task, long delay);

    /**
     *
     * @param taskName
     * @param beanName  bean of type {@link DynamicTaskRunnable}
     * @param arg
     * @param startTime
     * @param period
     * @return
     * @throws DuplicatedTaskException
     */
    ScheduledFuture<?> scheduleAtFixedRate(String taskName, String beanName, Serializable arg, Date startTime, long period) throws DuplicatedTaskException;

    void cancel(String taskName);

    void onDynamicTaskEvent(DynamicTaskEvent e);
}
