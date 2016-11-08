package org.everthrift.cassandra.scheduling;

import org.everthrift.cassandra.scheduling.context.SettableTriggerContext;
import org.springframework.scheduling.Trigger;
import org.springframework.scheduling.TriggerContext;

import java.util.Date;

public class DistributedPeriodicDynamicTrigger implements Trigger {


    public DistributedPeriodicDynamicTrigger() {

    }

    @Override
    public Date nextExecutionTime(TriggerContext triggerContext) {
        if (!(triggerContext instanceof SettableTriggerContext)) {
            throw new RuntimeException("triggerContext must be instanceof SettableTriggerContext");
        }

        if (triggerContext.lastScheduledExecutionTime() == null) {
            throw new RuntimeException("lastScheduledExecutionTime must not be null");
        }

        final Date now = new Date(System.currentTimeMillis());

        final long period = ((SettableTriggerContext) triggerContext).getPeriod();
        final Date next = new Date(triggerContext.lastScheduledExecutionTime().getTime() + period);

        return next.before(now) ? new Date(now.getTime() + period) : next;
    }
}
