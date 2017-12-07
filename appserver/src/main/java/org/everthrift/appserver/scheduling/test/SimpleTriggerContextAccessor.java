package org.everthrift.appserver.scheduling.test;

import org.everthrift.appserver.scheduling.context.SettableTriggerContext;
import org.everthrift.appserver.scheduling.context.SettableTriggerContextImpl;
import org.everthrift.appserver.scheduling.context.TriggerContextAccessor;
import org.jetbrains.annotations.NotNull;
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

    @NotNull
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
    public synchronized void updateLastCompletionTime(@NotNull Date time) {

        if (lastCompletionTime == null || lastCompletionTime.before(time)) {
            lastCompletionTime = time;
        }
    }

}
