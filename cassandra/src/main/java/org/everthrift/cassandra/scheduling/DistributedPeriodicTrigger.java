package org.everthrift.cassandra.scheduling;

import org.springframework.scheduling.TriggerContext;
import org.springframework.scheduling.support.PeriodicTrigger;

import java.util.Date;
import java.util.concurrent.TimeUnit;

public class DistributedPeriodicTrigger extends PeriodicTrigger {

    private final long period;

    public DistributedPeriodicTrigger(long period, TimeUnit timeUnit) {
        super(period, timeUnit);
        this.period = period;
    }

    public DistributedPeriodicTrigger(long period) {
        super(period);
        this.period = period;
    }

    @Override
    public Date nextExecutionTime(TriggerContext triggerContext) {
        //Мы не хотим, чтобы планировались пропущенные задачи в любом из режимов fixedDelay или fixedRate,
        //поэтому на планируем задачу в прошлом 
        final Date now = new Date(System.currentTimeMillis());
        final Date next = super.nextExecutionTime(triggerContext);
        return next.before(now) ? new Date(now.getTime() + period) : next;
    }
}
