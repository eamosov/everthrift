package org.everthrift.sql.hibernate.dao;

import com.google.common.collect.Sets;
import org.apache.commons.lang.ArrayUtils;
import org.everthrift.appserver.model.DaoEntityIF;
import org.everthrift.appserver.model.UpdatedAtIF;
import org.everthrift.utils.LongTimestamp;
import org.everthrift.utils.Pair;
import org.hibernate.EmptyInterceptor;
import org.hibernate.Transaction;
import org.hibernate.type.Type;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Set;

public class EntityInterceptor extends EmptyInterceptor {

    private final static Logger log = LoggerFactory.getLogger(EntityInterceptor.class);

    private static final long serialVersionUID = 1L;

    public static final EntityInterceptor INSTANCE = new EntityInterceptor();

    private final ThreadLocal<Set<Pair<String, Serializable>>> dirty = new ThreadLocal<Set<Pair<String, Serializable>>>() {
        @Override
        protected Set<Pair<String, Serializable>> initialValue() {
            return Sets.newHashSet();
        }
    };

    private EntityInterceptor() {

    }

    @Override
    public void afterTransactionBegin(Transaction tx) {
        log.debug("begin transaction");
        dirty.get().clear();
    }

    @Override
    public void afterTransactionCompletion(Transaction tx) {
        log.debug("end transaction");
        dirty.get().clear();
    }

    public boolean isDirty(@NotNull Object entity) {
        return dirty.get().contains(Pair.create(entity.getClass().getName(), ((DaoEntityIF) entity).getPk()));
    }

    @Override
    public boolean onFlushDirty(@NotNull Object entity, Serializable id, Object[] currentState, Object[] previousState, String[] propertyNames,
                                Type[] types) {

        if (log.isDebugEnabled()) {
            log.debug("onFlushDirty, class={}, object={}, id={}", entity.getClass()
                                                                        .getSimpleName(), System.identityHashCode(entity), id);
        }

        dirty.get().add(Pair.create(entity.getClass().getName(), ((DaoEntityIF) entity).getPk()));

        boolean updated = false;

        if (entity instanceof UpdatedAtIF) {

            final int idx = ArrayUtils.indexOf(propertyNames, "updatedAt");
            if (idx > 0) {
                final Long now = Long.valueOf(LongTimestamp.now());
                if (!now.equals(currentState[idx])) {
                    currentState[idx] = now;
                    updated = true;
                } else {
                    updated = false;
                }
            }
        }

        return updated;
    }

}
