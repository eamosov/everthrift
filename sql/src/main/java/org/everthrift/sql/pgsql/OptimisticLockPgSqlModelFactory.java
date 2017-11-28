package org.everthrift.sql.pgsql;

import com.google.common.base.Throwables;
import org.apache.thrift.TException;
import org.everthrift.appserver.model.DaoEntityIF;
import org.everthrift.appserver.model.EntityFactory;
import org.everthrift.appserver.model.EntityNotFoundException;
import org.everthrift.appserver.model.OptResult;
import org.everthrift.appserver.model.OptimisticLockModelFactoryIF;
import org.everthrift.appserver.model.UniqueException;
import org.everthrift.thrift.TFunction;
import org.everthrift.utils.Pair;
import org.everthrift.utils.tg.TimestampGenerator;
import org.hibernate.Session;
import org.hibernate.StaleStateException;
import org.hibernate.Transaction;
import org.hibernate.annotations.OptimisticLocking;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.ConcurrencyFailureException;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.Map;

public abstract class OptimisticLockPgSqlModelFactory<PK extends Serializable, ENTITY extends DaoEntityIF, E extends TException>
    extends AbstractPgSqlModelFactory<PK, ENTITY, E> implements OptimisticLockModelFactoryIF<PK, ENTITY, E> {

    @Autowired
    private TimestampGenerator timestampGenerator;

    /**
     * Cache need only because Hibernate does not cache rows selected by "IN"
     * statement
     *
     * @param cacheName
     * @param entityClass
     */
    public OptimisticLockPgSqlModelFactory(String cacheName, Class<ENTITY> entityClass) {
        super(cacheName, entityClass);

        if (entityClass.getAnnotation(OptimisticLocking.class) == null) {
            throw new RuntimeException("Class " + entityClass.getCanonicalName() + " must have OptimisticLocking annotation to use with OptimisticLockPgSqlModelFactory");
        }
    }

    @Override
    public final OptResult<ENTITY> updateUnchecked(PK id, TFunction<ENTITY, Boolean> mutator) {
        try {
            return update(id, mutator);
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public final OptResult<ENTITY> updateUnchecked(PK id, TFunction<ENTITY, Boolean> mutator, final EntityFactory<PK, ENTITY> factory) {
        try {
            return update(id, mutator, factory);
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public final OptResult<ENTITY> update(PK id, TFunction<ENTITY, Boolean> mutator) throws TException, E {
        return update(id, mutator, null);
    }

    @Override
    public final OptResult<ENTITY> update(PK id, TFunction<ENTITY, Boolean> mutator,
                                          final EntityFactory<PK, ENTITY> factory) throws TException, E {
        try {
            return optimisticUpdate(id, mutator, factory);
        } catch (EntityNotFoundException e) {
            throw createNotFoundException(id);
        }
    }

    private Pair<ENTITY, Long> tryOptimisticDelete(PK id) throws EntityNotFoundException {

        final Session session = getDao().getCurrentSession();
        final Transaction tx = session.beginTransaction();

        try {
            final ENTITY e = this.fetchEntityById(id);
            if (e == null) {
                throw new EntityNotFoundException(id);
            }

            this.getDao().delete(e);
            final long timestamp = timestampGenerator.next();
            tx.commit();
            return Pair.create(e, timestamp);
        } catch (Exception ex) {
            tx.rollback();
            throw ex;
        }
    }

    @Override
    public final OptResult<ENTITY> delete(final PK id) throws E {

        if (TransactionSynchronizationManager.isActualTransactionActive()) {
            throw new RuntimeException("Can't correctly do optimistic update within transaction");
        }

        try {
            final Pair<ENTITY, Long> deleted = OptimisticLockModelFactoryIF.optimisticUpdate((count) -> {
                try {
                    return tryOptimisticDelete(id);
                } catch (StaleStateException | ConcurrencyFailureException e) {
                    if (TransactionSynchronizationManager.isActualTransactionActive()) {
                        throw e;
                    }
                    log.debug("update fails id={}, let's try one more time? {}", id, e.getMessage());
                    return null;
                }
            });

            _invalidateEhCache(id, InvalidateCause.DELETE);

            final OptResult<ENTITY> r = OptResult.create(this, null, deleted.first, true, false, deleted.second);
            localEventBus.postEntityEvent(deleteEntityEvent(deleted.first));
            return r;
        } catch (EntityNotFoundException e) {
            _invalidateEhCache(id, InvalidateCause.DELETE);
            throw createNotFoundException(id);
        } catch (TException e) {
            _invalidateEhCache(id, InvalidateCause.DELETE);
            throw Throwables.propagate(e);
        } catch (Throwable e) {
            _invalidateEhCache(id, InvalidateCause.DELETE);
            throw e;
        }
    }

    @Override
    public final OptResult<ENTITY> optInsert(final ENTITY e) {

        final long timestamp = timestampGenerator.next();
        setCreatedAt(e, timestamp);

        final ENTITY inserted = getDao().save(e).first;
        _invalidateEhCache((PK) inserted.getPk(), InvalidateCause.INSERT);

        final OptResult<ENTITY> r = OptResult.create(this, inserted, null, true, true, timestamp);
        localEventBus.postEntityEvent(insertEntityEvent(inserted));
        return r;
    }

    @Override
    protected final ENTITY fetchEntityById(PK id) {
        final Session session = getDao().getCurrentSession();
        final Transaction tx = session.beginTransaction();

        try {
            final ENTITY e = super.fetchEntityById(id);
            tx.commit();
            return e;
        } catch (Exception ex) {
            tx.rollback();
            throw ex;
        }
    }

    @Override
    protected final Map<PK, ENTITY> fetchEntityByIdAsMap(Collection<PK> ids) {

        final Session session = getDao().getCurrentSession();
        final Transaction tx = session.beginTransaction();

        try {
            final Map<PK, ENTITY> e = super.fetchEntityByIdAsMap(ids);
            tx.commit();
            return e;
        } catch (Exception ex) {
            tx.rollback();
            throw ex;
        }
    }

    /**
     * @param id
     * @param mutator
     * @param factory
     * @return <new, old>
     * @throws Exception
     */
    private OptResult<ENTITY> optimisticUpdate(final PK id, final TFunction<ENTITY, Boolean> mutator,
                                               final EntityFactory<PK, ENTITY> factory) throws TException, EntityNotFoundException, StaleStateException, ConcurrencyFailureException {

        if (TransactionSynchronizationManager.isActualTransactionActive()) {
            throw new RuntimeException("Can't correctly do optimistic update within transaction");
        }

        final OptResult<ENTITY> ret = OptimisticLockModelFactoryIF.optimisticUpdate((count) -> {
            try {
                return tryOptimisticUpdate(id, mutator, factory);
            } catch (UniqueException e) {
                if (e.isPrimaryKey()) {
                    return null;
                } else {
                    throw Throwables.propagate(e);
                }
            } catch (StaleStateException | ConcurrencyFailureException e) {
                if (TransactionSynchronizationManager.isActualTransactionActive()) {
                    throw e;
                }
                log.debug("update fails id={}, let's try one more time? {}", id, e.getMessage());
                return null;
            }
        });

        if (ret.isUpdated) {
            _invalidateEhCache(id, ret.isInserted ? InvalidateCause.INSERT : InvalidateCause.UPDATE);
            localEventBus.postEntityEvent(updateEntityEvent(ret.beforeUpdate, ret.afterUpdate));
        }

        return ret;
    }

    private OptResult<ENTITY> tryOptimisticUpdate(PK id, TFunction<ENTITY, Boolean> mutator,
                                                  final EntityFactory<PK, ENTITY> factory) throws TException, EntityNotFoundException, StaleStateException, ConcurrencyFailureException {

        final Session session = getDao().getCurrentSession();
        final Transaction tx = session.beginTransaction();
        final long timestamp = timestampGenerator.next();

        try {
            try {
                ENTITY e;
                final ENTITY orig;

                final boolean isInserted;

                e = this.fetchEntityById(id);
                if (e == null) {
                    if (factory == null) {
                        throw new EntityNotFoundException(id);
                    }

                    orig = null;
                    e = factory.create(id);
                    isInserted = true;
                    setCreatedAt(e, timestamp);

                    getDao().persist(e);
                } else {
                    isInserted = false;
                    try {
                        orig = this.entityClass.getConstructor(this.entityClass).newInstance(e);
                    } catch (InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException
                        | NoSuchMethodException | SecurityException e1) {
                        throw Throwables.propagate(e1);
                    }
                }


                if (mutator.apply(e)) {
                    final Pair<ENTITY, Boolean> r = getDao().saveOrUpdate(e);
                    tx.commit();
                    return OptResult.create(this, r.first, orig, r.second, isInserted, timestamp);
                } else {
                    tx.commit();
                    return OptResult.create(this, e, e, false, false, timestamp);
                }
            } catch (EntityNotFoundException e) {
                log.debug("tryOptimisticUpdate ends with exception of type {}", e.getClass().getSimpleName());
                throw e;
            } catch (TException e) {
                log.warn("tryOptimisticUpdate ends with exception of type {}", e.getClass().getSimpleName());
                throw e;
            } catch (StaleStateException | ConcurrencyFailureException e) {
                throw e;
            } catch (Exception e) {
                log.warn("tryOptimisticUpdate ends with exception of type {}", e.getClass().getSimpleName());
                throw Throwables.propagate(e);
            }

        } catch (Exception ex) {
            tx.rollback();
            throw ex;
        }
    }
}
