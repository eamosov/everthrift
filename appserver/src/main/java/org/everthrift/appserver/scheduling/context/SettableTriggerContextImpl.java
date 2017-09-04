package org.everthrift.appserver.scheduling.context;

import java.io.Serializable;
import java.util.Date;

public class SettableTriggerContextImpl implements SettableTriggerContext {
    final long serial;

    private Date lastScheduledExecutionTime;
    private Date lastActualExecutionTime;
    private Date lastCompletionTime;

    private Long period;
    private String beanName;
    private Serializable arg;
    private boolean cancelled;

    public SettableTriggerContextImpl(long serial, Date lastScheduledExecutionTime, Date lastActualExecutionTime, Date lastCompletionTime,
                                      Long period, String beanName, Serializable arg, boolean cancelled) {
        super();
        this.serial = serial;
        this.lastScheduledExecutionTime = lastScheduledExecutionTime;
        this.lastActualExecutionTime = lastActualExecutionTime;
        this.lastCompletionTime = lastCompletionTime;

        this.period = period;
        this.beanName = beanName;
        this.arg = arg;
        this.cancelled = cancelled;
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

    @Override
    public Long getPeriod() {
        return period;
    }

    @Override
    public void setPeriod(Long period) {
        this.period = period;
    }

    @Override
    public String getBeanName() {
        return beanName;
    }

    @Override
    public Serializable getArg() {
        return arg;
    }

    @Override
    public boolean isCancelled() {
        return cancelled;
    }

    @Override
    public void setCancelled(boolean cancelled) {
        this.cancelled = cancelled;
    }
}