package org.everthrift.sql.pgsql;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListeningExecutorService;
import net.sf.ehcache.Cache;
import org.apache.thrift.TException;
import org.everthrift.appserver.model.DaoEntityIF;
import org.everthrift.appserver.model.EntityFactory;
import org.everthrift.appserver.model.EntityNotFoundException;
import org.everthrift.appserver.model.LocalEventBus;
import org.everthrift.appserver.model.OptResult;
import org.everthrift.appserver.model.OptimisticLockModelFactoryIF;
import org.everthrift.appserver.model.UniqueException;
import org.everthrift.thrift.TFunction;
import org.everthrift.utils.LongTimestamp;
import org.everthrift.utils.Pair;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.StaleStateException;
import org.hibernate.Transaction;
import org.hibernate.annotations.OptimisticLocking;
import org.hibernate.criterion.Restrictions;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.dao.ConcurrencyFailureException;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

public abstract class OptimisticLockPgSqlModelFactory<PK extends Serializable, ENTITY extends DaoEntityIF, E extends TException>
    extends AbstractPgSqlModelFactory<PK, ENTITY, E> implements OptimisticLockModelFactoryIF<PK, ENTITY, E> {

    /**
     * Cache need only because Hibernate does not cache rows selected by "IN"
     * statement
     *
     * @param cacheName
     * @param entityClass
     */
    public OptimisticLockPgSqlModelFactory(String cacheName, Class<ENTITY> entityClass,
                                           @Qualifier("listeningCallerRunsBoundQueueExecutor") ListeningExecutorService listeningExecutorService,
                                           SessionFactory sessionFactory,
                                           LocalEventBus localEventBus) {
        super(cacheName, entityClass, listeningExecutorService, sessionFactory, localEventBus);

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

    private ENTITY tryOptimisticDelete(PK id) throws EntityNotFoundException {

        final Session session = getDao().getCurrentSession();
        final Transaction tx = session.beginTransaction();

        try {
            final ENTITY e = this.fetchEntityById(id);
            if (e == null) {
                throw new EntityNotFoundException(id);
            }

            this.getDao().delete(e);
            tx.commit();
            return e;
        }catch (Exception ex){
            tx.rollback();
            throw  ex;
        }
    }

    @Override
    public final OptResult<ENTITY> delete(final PK id) throws E {

        if (TransactionSynchronizationManager.isActualTransactionActive()) {
            throw new RuntimeException("Can't correctly do optimistic update within transaction");
        }

        try {
            final ENTITY deleted = OptimisticLockModelFactoryIF.optimisticUpdate((count) -> {
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

            final OptResult<ENTITY> r = OptResult.create(this, null, deleted, true);
            localEventBus.postEntityEvent(deleteEntityEvent(deleted));
            return r;
        } catch (EntityNotFoundException e) {
            throw createNotFoundException(id);
        } catch (TException e) {
            throw Throwables.propagate(e);
        } finally {
            _invalidateEhCache(id);
        }
    }

    @Override
    public final ENTITY insertEntity(ENTITY e) {
        try {
            return this.optInsert(e).afterUpdate;
        } catch (Exception e1) {
            throw Throwables.propagate(e1);
        }
    }

    @Override
    public final OptResult<ENTITY> optInsert(final ENTITY e) {

        final long now = LongTimestamp.now();

        setCreatedAt(e);

        final ENTITY inserted = getDao().save(e).first;
        _invalidateEhCache((PK) inserted.getPk());

        final OptResult<ENTITY> r = OptResult.create(this, inserted, null, true);
        localEventBus.postEntityEvent(insertEntityEvent(inserted));
        return r;
    }

    @Override
    protected final ENTITY fetchEntityById(PK id) {
        final Session session = getDao().getCurrentSession();
        final Transaction tx = session.beginTransaction();

        try{
            final ENTITY e = super.fetchEntityById(id);
            tx.commit();
            return e;
        }catch (Exception ex){
            tx.rollback();
            throw ex;
        }
    }

    @Override
    protected final Map<PK, ENTITY> fetchEntityByIdAsMap(Collection<PK> ids) {

        final Session session = getDao().getCurrentSession();
        final Transaction tx = session.beginTransaction();

        try{
            final Map<PK, ENTITY>  e = super.fetchEntityByIdAsMap(ids);
            tx.commit();
            return e;
        }catch (Exception ex){
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
            _invalidateEhCache(id);

            localEventBus.postEntityEvent(updateEntityEvent(ret.beforeUpdate, ret.afterUpdate));
        }

        return ret;
    }

    private OptResult<ENTITY> tryOptimisticUpdate(PK id, TFunction<ENTITY, Boolean> mutator,
                                                  final EntityFactory<PK, ENTITY> factory) throws TException, EntityNotFoundException, StaleStateException, ConcurrencyFailureException {

        final Session session = getDao().getCurrentSession();
        final Transaction tx = session.beginTransaction();

        try{
            try {
                ENTITY e;
                final ENTITY orig;

                e = this.fetchEntityById(id);
                if (e == null) {
                    if (factory == null) {
                        throw new EntityNotFoundException(id);
                    }

                    orig = null;
                    e = factory.create(id);
                    setCreatedAt(e);

                    getDao().persist(e);
                } else {
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
                    return OptResult.create(this, r.first, orig, r.second);
                } else {
                    tx.commit();
                    return OptResult.create(this, e, e, false);
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

        }catch (Exception ex){
            tx.rollback();
            throw ex;
        }
    }

    @Override
    public final ENTITY updateEntity(ENTITY e, ENTITY old) throws UniqueException {

        final Session session = getDao().getCurrentSession();
        final Transaction tx = session.beginTransaction();

        try {
            final ENTITY before;
            if (e.getPk() != null) {
                before = getDao().findById((PK) e.getPk());
            } else {
                before = null;
            }

            final Pair<ENTITY, Boolean> r = getDao().saveOrUpdate(e);
            tx.commit();

            final OptResult<ENTITY> ret = new OptResult<>(this, r.first, before, true);

            if (r.second) {
                localEventBus.postEntityEvent(updateEntityEvent(before, r.first));
            }
            return r.first;
        }catch (Exception ex){
            tx.rollback();
            throw  ex;
        }
    }
}
