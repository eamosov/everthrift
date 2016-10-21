package org.everthrift.cassandra.scheduling;

import org.springframework.scheduling.Trigger;

import java.util.Date;
import java.util.concurrent.ScheduledFuture;

public interface DistributedTaskScheduler {

    ScheduledFuture<?> schedule(String taskName, Runnable task, Trigger trigger);

    ScheduledFuture<?> scheduleAtFixedRate(String taskName, Runnable task, long period);

    ScheduledFuture<?> scheduleAtFixedRate(String taskName, Runnable task, Date startTime, long period);

    ScheduledFuture<?> scheduleWithFixedDelay(String taskName, Runnable task, Date startTime, long delay);

    ScheduledFuture<?> scheduleWithFixedDelay(String taskName, Runnable task, long delay);

}
