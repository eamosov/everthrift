package org.everthrift.cassandra.scheduling.cassandra;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.common.base.Throwables;
import org.everthrift.cassandra.scheduling.ContextAccessError;
import org.everthrift.cassandra.scheduling.context.SettableTriggerContext;
import org.everthrift.cassandra.scheduling.context.SettableTriggerContextImpl;
import org.everthrift.cassandra.scheduling.context.TriggerContextAccessor;
import org.everthrift.utils.ClassUtils;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Date;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.insertInto;
import static com.datastax.driver.core.querybuilder.QueryBuilder.select;
import static com.datastax.driver.core.querybuilder.QueryBuilder.set;

class TriggerContextAccessorImpl implements TriggerContextAccessor {

    private final CasTriggerContextAccessorFactory casTriggerContextAccessorFactory;
    final String taskName;
    final boolean isDynamic;

    TriggerContextAccessorImpl(CasTriggerContextAccessorFactory casTriggerContextAccessorFactory, String taskName, boolean isDynamic) {
        super();
        this.casTriggerContextAccessorFactory = casTriggerContextAccessorFactory;
        this.taskName = taskName;
        this.isDynamic = isDynamic;
    }

    private SettableTriggerContext _get() {
        final Row r = this.casTriggerContextAccessorFactory.getSession()
                                                           .execute(select().from(this.casTriggerContextAccessorFactory.getTableName())
                                                                            .where(QueryBuilder.eq("id", taskName)))
                                                           .one();
        if (r == null) {
            return null;
        }

        final long serial = r.getLong("serial");
        final Date lastScheduledExecutionTime = r.getTimestamp("last_scheduled_execution_time");
        final Date lastActualExecutionTime = r.getTimestamp("last_actual_execution_time");
        final Date lastCompletionTime = r.getTimestamp("last_completion_time");

        final long period = r.getLong("period");
        final String beanName = r.getString("bean_name");
        final Serializable arg;
        try {
            final ByteBuffer bb = r.getBytes("arg");
            arg = bb != null ? (Serializable) ClassUtils.readObject(bb) : null;
        } catch (ClassNotFoundException e) {
            throw Throwables.propagate(e);
        }
        final boolean cancelled = r.getBool("cancelled");

        return new SettableTriggerContextImpl(serial, lastScheduledExecutionTime, lastActualExecutionTime, lastCompletionTime,
                                              period, beanName, arg, cancelled);
    }

    private boolean _insert() {
        return this.casTriggerContextAccessorFactory.getSession()
                                                    .execute(insertInto(this.casTriggerContextAccessorFactory.getTableName())
                                                                 .value("id", taskName)
                                                                 .value("serial", 0)
                                                                 .value("last_completion_time", new Date())
                                                                 .ifNotExists())
                                                    .wasApplied();
    }

    @Override
    public SettableTriggerContext get() throws ContextAccessError {
        if (isDynamic) {
            return _get();
        } else {
            try {
                SettableTriggerContext ctx;
                while ((ctx = _get()) == null) {
                    _insert();
                }
                return ctx;
            } catch (Exception e) {
                throw new ContextAccessError(e);
            }
        }
    }

    @Override
    public boolean update(SettableTriggerContext ctx) throws ContextAccessError {
        try {
            final SettableTriggerContextImpl _ctx = (SettableTriggerContextImpl) ctx;
            return this.casTriggerContextAccessorFactory.getSession()
                                                        .execute(QueryBuilder.update(this.casTriggerContextAccessorFactory
                                                                                         .getTableName())
                                                                             .with(set("last_scheduled_execution_time", _ctx
                                                                                 .lastScheduledExecutionTime()))
                                                                             .and(set("last_actual_execution_time", _ctx
                                                                                 .lastActualExecutionTime()))
                                                                             .and(set("last_completion_time", _ctx
                                                                                 .lastCompletionTime()))
                                                                             .and(set("serial", _ctx.getSerial() + 1))
                                                                             .and(set("cancelled", _ctx.isCancelled()))
                                                                             .where(eq("id", taskName))
                                                                             .onlyIf(QueryBuilder.eq("serial", _ctx
                                                                                 .getSerial())))
                                                        .wasApplied();
        } catch (Exception e) {
            throw new ContextAccessError(e);
        }
    }

    @Override
    public void updateLastCompletionTime(Date time) throws ContextAccessError {
        try {
            this.casTriggerContextAccessorFactory.getSession()
                                                 .execute(QueryBuilder.update(this.casTriggerContextAccessorFactory.getTableName())
                                                                      .with(set("last_completion_time", time))
                                                                      .where(eq("id", taskName))
                                                                      .onlyIf(QueryBuilder.lt("last_completion_time", time)));
        } catch (Exception e) {
            throw new ContextAccessError(e);
        }
    }
}