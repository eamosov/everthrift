package org.everthrift.appserver.model;

import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
import org.jetbrains.annotations.NotNull;
import org.springframework.util.CollectionUtils;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class OptResult<ENTITY extends DaoEntityIF> {

    public static final OptResult CANCELED = OptResult.create(null, null, null, false, false, 0);

    public final OptimisticLockModelFactoryIF<?, ENTITY, ?> factory;

    public final ENTITY afterUpdate;

    public final ENTITY beforeUpdate;

    public final boolean isUpdated; // true, если произошло изменение объекта в БД

    public final boolean isInserted;

    public final long timestamp; //updated timestamp in mks

    public List<OptResult> inner;

    public OptResult(OptimisticLockModelFactoryIF<?, ENTITY, ?> factory, ENTITY afterUpdate, ENTITY beforeUpdate,
                     boolean isUpdated, boolean isInserted, long timestamp) {
        super();
        this.afterUpdate = afterUpdate;
        this.beforeUpdate = beforeUpdate;
        this.isUpdated = isUpdated;
        this.isInserted = isInserted;
        this.factory = factory;
        this.timestamp = timestamp;
    }

    @NotNull
    public static <ENTITY extends DaoEntityIF> OptResult<ENTITY> notUpdated(OptimisticLockModelFactoryIF<?, ENTITY, ?> factory,
                                                                            ENTITY updated, long timestamp) {
        return create(factory, updated, updated, false, false, timestamp);
    }

    @NotNull
    public static <ENTITY extends DaoEntityIF> OptResult<ENTITY> create(OptimisticLockModelFactoryIF<?, ENTITY, ?> factory,
                                                                        ENTITY afterUpdate, ENTITY beforeUpdate,
                                                                        boolean isUpdated, boolean isInserted,
                                                                        long timestamp) {

        return new OptResult<ENTITY>(factory, afterUpdate, beforeUpdate, isUpdated, isInserted, timestamp);
    }

    public boolean isCanceled() {
        return this == CANCELED;
    }

    public boolean isInserted() {
        return isUpdated && beforeUpdate == null;
    }

    public <T extends DaoEntityIF> void add(OptResult<T> e) {
        if (inner == null) {
            inner = Lists.newArrayList();
        }

        inner.add(e);
    }

    public <PK extends Serializable, T extends DaoEntityIF> Collection<OptResult> getInner(OptimisticLockModelFactoryIF<PK, T, ?> factory) {
        if (CollectionUtils.isEmpty(inner)) {
            return Collections.emptyList();
        }

        return Collections2.filter(inner, r -> (r.factory == factory));
    }

    @NotNull
    public <PK extends Serializable, T extends DaoEntityIF> T getInnerUpdated(OptimisticLockModelFactoryIF<PK, T, ?> factory, @NotNull T defaultValue) {

        if (CollectionUtils.isEmpty(inner)) {
            return defaultValue;
        }

        for (OptResult r : inner) {
            if (r.factory == factory && r.afterUpdate != null && r.afterUpdate.getPk().equals(defaultValue.getPk())) {
                return (T) r.afterUpdate;
            }
        }
        return defaultValue;
    }

    @NotNull
    @Override
    public String toString() {
        return "OptResult [factory=" + factory + ", afterUpdate=" + afterUpdate + ", beforeUpdate=" + beforeUpdate + ", isUpdated="
            + isUpdated + ", inner=" + inner + "]";
    }
}