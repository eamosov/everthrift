package org.everthrift.cassandra.scheduling.context;

import java.util.Date;

public class SettableTriggerContextImpl implements SettableTriggerContext {
    final long serial;

    private Date lastScheduledExecutionTime;
    private Date lastActualExecutionTime;
    private Date lastCompletionTime;

    public SettableTriggerContextImpl(long serial, Date lastScheduledExecutionTime, Date lastActualExecutionTime, Date lastCompletionTime) {
        super();
        this.serial = serial;
        this.lastScheduledExecutionTime = lastScheduledExecutionTime;
        this.lastActualExecutionTime = lastActualExecutionTime;
        this.lastCompletionTime = lastCompletionTime;
    }

    @Override
    public Date lastScheduledExecutionTime() {
        return lastScheduledExecutionTime;
    }

    @Override
    public Date lastActualExecutionTime() {
        return lastActualExecutionTime;
    }

    @Override
    public Date lastCompletionTime() {
        return lastCompletionTime;
    }

    @Override
    public void setLastScheduledExecutionTime(Date lastScheduledExecutionTime) {
        this.lastScheduledExecutionTime = lastScheduledExecutionTime;

    }

    @Override
    public void setLastActualExecutionTime(Date lastActualExecutionTime) {
        this.lastActualExecutionTime = lastActualExecutionTime;

    }

    @Override
    public void setLastCompletionTime(Date lastCompletionTime) {
        this.lastCompletionTime = lastCompletionTime;
    }

    public long getSerial() {
        return serial;
    }
}