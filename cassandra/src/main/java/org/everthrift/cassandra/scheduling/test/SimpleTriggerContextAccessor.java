package org.everthrift.cassandra.scheduling.test;

import org.everthrift.cassandra.scheduling.context.SettableTriggerContext;
import org.everthrift.cassandra.scheduling.context.SettableTriggerContextImpl;
import org.everthrift.cassandra.scheduling.context.TriggerContextAccessor;
import org.springframework.util.Assert;

import java.util.Date;

/**
 * Тестовая реализация TriggerContextAccessor, хранящего  TriggerContext в памяти
 *
 * @author fluder
 */
public class SimpleTriggerContextAccessor implements TriggerContextAccessor {

    private long serial;

    private Date lastScheduledExecutionTime;

    private Date lastActualExecutionTime;

    private Date lastCompletionTime;

    public SimpleTriggerContextAccessor(Date lastScheduledExecutionTime, Date lastActualExecutionTime, Date lastCompletionTime) {
        super();
        this.lastScheduledExecutionTime = lastScheduledExecutionTime;
        this.lastActualExecutionTime = lastActualExecutionTime;
        this.lastCompletionTime = lastCompletionTime;
    }

    @Override
    public synchronized SettableTriggerContext get() {
        return new SettableTriggerContextImpl(serial, lastScheduledExecutionTime, lastActualExecutionTime, lastCompletionTime,
                                              null, null, null, false);
    }

    @Override
    public synchronized boolean update(SettableTriggerContext ctx) {
        Assert.isInstanceOf(SettableTriggerContextImpl.class, ctx);

        final SettableTriggerContextImpl _ctx = (SettableTriggerContextImpl) ctx;
        if (_ctx.getSerial() != serial) {
            return false;
        }

        this.lastScheduledExecutionTime = _ctx.lastScheduledExecutionTime();
        this.lastActualExecutionTime = _ctx.lastActualExecutionTime();
        this.lastCompletionTime = _ctx.lastCompletionTime();
        this.serial++;
        return true;
    }

    @Override
    public synchronized void updateLastCompletionTime(Date time) {

        if (lastCompletionTime == null || lastCompletionTime.before(time)) {
            lastCompletionTime = time;
        }
    }

}
