package org.everthrift.appserver.scheduling.zoo;

import com.google.common.base.Throwables;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.everthrift.appserver.scheduling.ContextAccessError;
import org.everthrift.appserver.scheduling.context.SettableTriggerContext;
import org.everthrift.appserver.scheduling.context.SettableTriggerContextImpl;
import org.everthrift.appserver.scheduling.context.TriggerContextAccessor;
import org.everthrift.utils.ClassUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.Serializable;
import java.util.Date;

/**
 * Created by fluder on 06/09/17.
 */
public class ZooTriggerContextAccessorImpl implements TriggerContextAccessor {

    private final ZooTriggerContextAccessorFactory factory;
    final String taskName;
    final boolean isDynamic;

    public ZooTriggerContextAccessorImpl(ZooTriggerContextAccessorFactory factory, String taskName, boolean isDynamic) {
        this.factory = factory;
        this.taskName = taskName;
        this.isDynamic = isDynamic;
    }

    private SettableTriggerContext _get() {

        final Stat stat = new Stat();
        final ZooData zooData;

        try {
            zooData = factory.gson.fromJson(new String(factory.curator.getData()
                                                                      .storingStatIn(stat)
                                                                      .forPath(factory.path(taskName))), ZooData.class);
        } catch (KeeperException.NoNodeException e) {
            return null;
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }

        final Serializable arg;
        try {
            arg = zooData.arg != null ? (Serializable) ClassUtils.readObject(zooData.arg) : null;
        } catch (ClassNotFoundException e) {
            throw Throwables.propagate(e);
        }

        return new SettableTriggerContextImpl(stat.getVersion(),
                                              zooData.lastScheduledExecutionTime,
                                              zooData.lastActualExecutionTime,
                                              zooData.lastCompletionTime,
                                              zooData.period,
                                              zooData.beanName,
                                              arg,
                                              zooData.cancelled);
    }

    private boolean _insert() {

        final ZooData zooData = new ZooData();
        zooData.lastCompletionTime = new Date();

        try {
            factory.curator.create()
                           .creatingParentsIfNeeded()
                           .forPath(factory.path(taskName), factory.gson.toJson(zooData).getBytes());

            return true;
        } catch (KeeperException.NodeExistsException e) {
            return false;
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    @Nullable
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
    public boolean update(@NotNull SettableTriggerContext ctx) throws ContextAccessError {

        final ZooData zooData = new ZooData();
        zooData.lastScheduledExecutionTime = ctx.lastScheduledExecutionTime();
        zooData.lastActualExecutionTime = ctx.lastActualExecutionTime();
        zooData.lastCompletionTime = ctx.lastCompletionTime();
        zooData.cancelled = ctx.isCancelled();
        zooData.period = ctx.getPeriod();

        try {
            factory.curator.setData()
                           .withVersion((int) ((SettableTriggerContextImpl) ctx).getSerial())
                           .forPath(factory.path(taskName), factory.gson.toJson(zooData).getBytes());
            return true;
        } catch (@NotNull KeeperException.BadVersionException | KeeperException.NoNodeException e) {
            return false;
        } catch (Exception e) {
            throw new ContextAccessError(e);
        }
    }

    @Override
    public void updateLastCompletionTime(@NotNull Date time) throws ContextAccessError {


        boolean badVersion;
        do {
            badVersion = false;

            try {
                final Stat stat = new Stat();
                final ZooData zooData = factory.gson.fromJson(new String(factory.curator.getData()
                                                                                        .storingStatIn(stat)
                                                                                        .forPath(factory.path(taskName))), ZooData.class);

                if (zooData.lastCompletionTime.before(time)) {
                    zooData.lastCompletionTime = time;
                } else {
                    break;
                }

                factory.curator.setData()
                               .withVersion(stat.getVersion())
                               .forPath(factory.path(taskName), factory.gson.toJson(zooData).getBytes());

            } catch (KeeperException.BadVersionException e) {
                badVersion = true;
            } catch (Exception e) {
                throw new ContextAccessError(e);
            }

        } while (badVersion);

    }
}
