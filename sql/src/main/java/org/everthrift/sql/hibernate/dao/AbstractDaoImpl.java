package org.everthrift.sql.hibernate.dao;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import org.everthrift.appserver.model.CreatedAtIF;
import org.everthrift.appserver.model.DaoEntityIF;
import org.everthrift.appserver.model.UniqueException;
import org.everthrift.utils.Pair;
import org.hibernate.CacheMode;
import org.hibernate.Criteria;
import org.hibernate.FlushMode;
import org.hibernate.HibernateException;
import org.hibernate.LockMode;
import org.hibernate.Query;
import org.hibernate.SQLQuery;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.criterion.Criterion;
import org.hibernate.criterion.Order;
import org.hibernate.criterion.Projection;
import org.hibernate.criterion.ProjectionList;
import org.hibernate.criterion.Projections;
import org.hibernate.criterion.Restrictions;
import org.hibernate.exception.ConstraintViolationException;
import org.hibernate.persister.entity.AbstractEntityPersister;
import org.hibernate.query.NativeQuery;
import org.hibernate.transform.ResultTransformer;
import org.hibernate.type.Type;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;

import javax.persistence.PersistenceException;
import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class AbstractDaoImpl<K extends Serializable, V extends DaoEntityIF> implements AbstractDao<K, V> {

    private static final Logger log = LoggerFactory.getLogger(AbstractDaoImpl.class);

    private SessionFactory sessionFactory;

    protected final Class<V> entityClass;

    private Executor executor;

    private static final Pattern pkey = Pattern.compile("^[^_]+_pkey$");

    private static final Pattern p = Pattern.compile("^[^_]+_([^_]+)_[^_]+$");

    private CacheMode cacheMode = CacheMode.NORMAL;

    public AbstractDaoImpl(Class<V> entityClass) {
        this.entityClass = entityClass;
    }

    private AbstractDaoImpl(SessionFactory sessionFactory, Class<V> entityClass, Executor executor) {
        this.sessionFactory = sessionFactory;
        this.entityClass = entityClass;
        this.executor = executor;
    }

    @Override
    public void setSessionFactory(SessionFactory sessionFactory) {
        this.sessionFactory = sessionFactory;
    }

    public CacheMode getCacheMode() {
        return cacheMode;
    }

    public void setCacheMode(CacheMode cacheMode) {
        this.cacheMode = cacheMode;
    }

    @Override
    public Session getCurrentSession() {
        try {
            return sessionFactory.getCurrentSession();
        } catch (HibernateException e) {
            final Session s = sessionFactory.openSession();
            s.setCacheMode(cacheMode);
            return s;
        }
    }

    private static class TxWrap {
        @NotNull
        private final Transaction tx;
        private final Session session;
        private final boolean isActive;

        TxWrap(@NotNull Session session) {
            this.session = session;
            this.tx = session.getTransaction();
            this.isActive = tx.isActive();
        }

        @NotNull
        TxWrap begin(boolean readOnly) {
            if (!isActive) {
                tx.begin();
                if (readOnly) {
                    session.setDefaultReadOnly(true);
                    session.setHibernateFlushMode(FlushMode.MANUAL);
                }
            }
            return this;
        }

        void rollback() {
            tx.rollback();
        }

        void commit() {
            if (!isActive) {
                tx.commit();
            }
        }
    }

    @NotNull
    private TxWrap beginTransaction(@NotNull Session session, boolean readOnly) {
        return new TxWrap(session).begin(readOnly);
    }

    public Executor getExecutor() {
        return executor;
    }

    @Override
    public void setExecutor(Executor executor) {
        this.executor = executor;
    }

    @Override
    public V findById(K id) {

        final Session session = getCurrentSession();

        if (log.isDebugEnabled()) {
            log.debug("findById tx={}: {}#{}", session.getTransaction()
                                                      .getStatus(), entityClass.getSimpleName(), id);
        }

        return session.get(entityClass, id);
    }

    @Override
    public Collection<V> findByIds(@NotNull Collection<K> ids) {
        return this.findByCriteria(Restrictions.in("id", ids));
    }

    @Nullable
    private UniqueException uniqueException(@NotNull ConstraintViolationException e) {

        final String fieldName;
        final boolean isPrimaryKey;

        if (e.getConstraintName() != null) {

            final Matcher pm = pkey.matcher(e.getConstraintName());
            if (pm.matches()) {
                fieldName = null;
                isPrimaryKey = true;
            } else {
                isPrimaryKey = false;
                final Matcher m = p.matcher(e.getConstraintName());
                if (m.matches()) {
                    fieldName = m.group(1);
                } else {
                    log.error("Coudn't parse constraint name:{}", e.getConstraintName());
                    fieldName = null;
                }
            }

        } else {
            fieldName = null;
            isPrimaryKey = false;
        }

        return new UniqueException(fieldName, isPrimaryKey, null);
    }

    @Override
    public void persist(V e) throws UniqueException {
        try {

            tx(session -> {
                session.persist(e);
            }, false);

        } catch (Exception e1) {
            throw convert(e1);
        }
    }

    @Override
    public Pair<V, Boolean> saveOrUpdate(@NotNull V e) throws UniqueException {

        try {
            if (log.isDebugEnabled()) {
                log.debug("saveOrUpdate, class={}, object={}, id={}", e.getClass()
                                                                       .getSimpleName(), System.identityHashCode(e), e.getPk());
            }

            return tx(session -> {
                if (log.isDebugEnabled()) {
                    log.debug("saveOrUpdate tx={}: {}", session.getTransaction().getStatus(), e);
                }

                if (e.getPk() == null) {
                    e.setPk(session.save(e));
                    session.refresh(e);
                    return Pair.create((V) session.get(entityClass, e.getPk()), true);
                } else {
                    V ret = (V) session.merge(e);
                    if (session.isDirty()) {
                        session.flush();
                    }
                    return Pair.create(ret, EntityInterceptor.INSTANCE.isDirty(e));
                }
            }, false);
        } catch (Exception e1) {
            throw convert(e1);
        }
    }

    @Override
    public Pair<V, Boolean> save(@NotNull V e) throws UniqueException {

        try {
            if (log.isDebugEnabled()) {
                log.debug("save, class={}, object={}, id={}", e.getClass()
                                                               .getSimpleName(), System.identityHashCode(e), e.getPk());
            }

            return tx(session -> {
                if (log.isDebugEnabled()) {
                    log.debug("save tx={}: {}", session.getTransaction().getStatus(), e);
                }

                CreatedAtIF.setCreatedAt(e);

                if (e.getPk() == null) {
                    e.setPk(session.save(e));
                    session.refresh(e);
                    return Pair.create((V) session.get(entityClass, e.getPk()), true);
                } else {
                    V ret = (V) session.merge(e);
                    if (session.isDirty()) {
                        session.flush();
                    }
                    return Pair.create(ret, EntityInterceptor.INSTANCE.isDirty(e));
                }
            }, false);
        } catch (Exception e1) {
            throw convert(e1);
        }
    }

    private void tx(@NotNull Consumer<Session> r, boolean readOnly) {
        final Session session = getCurrentSession();
        final TxWrap tx = beginTransaction(session, readOnly);
        try {
            r.accept(session);
            tx.commit();
        } catch (Exception ex) {
            tx.rollback();
            throw ex;
        }
    }

    public <R> R tx(@NotNull Function<Session, R> r, boolean readOnly) {
        final Session session = getCurrentSession();
        final TxWrap tx = beginTransaction(session, readOnly);
        try {
            final R result = r.apply(session);
            tx.commit();
            return result;
        } catch (Exception ex) {
            tx.rollback();
            throw ex;
        }
    }


    @Override
    public void delete(V e) {
        tx(session -> {
            if (log.isDebugEnabled()) {
                log.debug("delete tx={}: {}", session.getTransaction().getStatus(), e);
            }

            session.delete(e);
        }, false);
    }

    @Override
    public void deleteAll() {

        tx(session -> {
            session.createQuery(
                "DELETE FROM " + ((AbstractEntityPersister) sessionFactory.getClassMetadata(entityClass)).getEntityName())
                   .executeUpdate();
        }, false);
    }

    public int deleteByCriteria(Criterion criterion) {

        return tx(session -> {
            final List<V> ee = findByCriteria(criterion);

            if (!CollectionUtils.isEmpty(ee)) {
                ee.forEach(this::delete);
                return ee.size();
            }

            return 0;
        }, false);
    }

    @Override
    public Object uniqueResult(Criterion criterion, @NotNull Projection... projections) {
        return tx(session -> {
            final Criteria criteria = session.createCriteria(entityClass);

            criteria.add(criterion);
            ProjectionList projectionList = Projections.projectionList();
            for (Projection p : projections) {
                projectionList = projectionList.add(p);
            }
            criteria.setProjection(projectionList);
            return criteria.uniqueResult();
        }, true);
    }

    @NotNull
    @Override
    public List<K> findPkByCriteria(Criterion criterion) {
        return (List) findByCriteria(criterion, Projections.id(), null, null, null, null);
    }

    @Override
    public List<V> findByCriteria(Criterion criterion) {
        return findByCriteria(criterion, null, null, null, null, null);
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<V> findByCriteria(Criterion criterion, @Nullable Projection proj, @Nullable LockMode lockMode, @Nullable List<Order> order, @Nullable Integer limit,
                                  @Nullable Integer offset) {

        return tx(session -> {
            final Criteria criteria = session.createCriteria(entityClass);

            criteria.add(criterion);

            if (!CollectionUtils.isEmpty(order)) {
                for (Order o : order) {
                    if (o != null) {
                        criteria.addOrder(o);
                    }
                }
            }

            if (proj != null) {
                criteria.setProjection(proj);
            }

            if (lockMode != null) {
                criteria.setLockMode(lockMode);
            }

            if (limit != null) {
                criteria.setMaxResults(limit);
            }

            if (offset != null) {
                criteria.setFirstResult(offset);
            }

            return Lists.newArrayList(criteria.list());
        }, true);
    }

    @Override
    public <R> R withSession(@NotNull Function<Session, R> r, boolean readOnly) {
        try {
            return tx(r, readOnly);
        } catch (Exception e) {
            throw convert(e);
        }
    }

    /**
     * @param query             sql query : "?" - param placeholder
     * @param mapping           ResultSet mapping --> columnName -> columnType, may be
     *                          null
     * @param resultTransformer transformer for resultSet, may be null
     * @param params            query params
     * @return
     * @see org.hibernate.transform.Transformers Если не передан параметр
     * mapping то будет использоваться маппер и трансформер класса entityClass
     */
    @Override
    public <X> List<X> findBySQLQuery(String query, @Nullable Map<String, Type> mapping, @Nullable ResultTransformer resultTransformer, @NotNull Object... params) {

        return tx(session -> {
            final NativeQuery<X> q = session.createSQLQuery(query);

            if (log.isDebugEnabled()) {
                log.debug("findBySQLQuery tx={}: {}#{}", session.getTransaction()
                                                                .getStatus(), entityClass.getSimpleName(), query);
            }

            for (int i = 0; i < params.length; i++) {
                q.setParameter(i, params[i]);
            }
            if (mapping != null) {
                for (String columnName : mapping.keySet()) {
                    q.addScalar(columnName, mapping.get(columnName));
                }
            } else {
                q.addEntity(entityClass);
            }
            if (resultTransformer != null && mapping != null) {
                q.setResultTransformer(resultTransformer);
            }

            return q.list();
        }, true);
    }

    @Override
    public int executeCustomUpdate(K evictId, String query, @NotNull final Object... params) {

        return executeCustomUpdate(evictId, query, new Function<SQLQuery, Query>() {

            @NotNull
            @Override
            public Query apply(@NotNull SQLQuery q) {

                for (int i = 0; i < params.length; i++) {
                    q.setParameter(i, params[i]);
                }

                return q;
            }
        });
    }

    @NotNull
    private RuntimeException convert(Exception e) {
        if (e instanceof ConstraintViolationException) {
            throw uniqueException((ConstraintViolationException) e);
        } else if (e instanceof PersistenceException && e.getCause() instanceof ConstraintViolationException) {
            throw uniqueException((ConstraintViolationException) e.getCause());
        }

        throw Throwables.propagate(e);
    }

    @Override
    public int executeCustomUpdate(@Nullable K evictId, String query, @NotNull Function<SQLQuery, Query> bindFunction) {
        try {

            return tx(session -> {
                final SQLQuery q = session.createSQLQuery(query);

                if (log.isDebugEnabled()) {
                    log.debug("executeCustomUpdate tx={}: {}#{}", session.getTransaction()
                                                                         .getStatus(), entityClass.getSimpleName(), query);
                }

                bindFunction.apply(q);

                try {
                    return q.executeUpdate();
                } finally {
                    if (evictId != null) {
                        sessionFactory.getCache().evictEntity(entityClass, evictId);
                    }
                }
            }, false);
        } catch (Exception e) {
            throw convert(e);
        }
    }

    private void propogateIfAbsentMessage(@NotNull Throwable e, String message) {
        if (!e.getMessage().equals(message)) {
            throw Throwables.propagate(e);
        }
    }

    @NotNull
    @Override
    public Map<K, V> findByIdsAsMap(@NotNull Collection<K> id) {
        final Map<K, V> ret = new HashMap<K, V>();
        final Collection<V> rs = findByIds(id);
        for (V r : rs) {
            ret.put((K) r.getPk(), r);
        }
        for (K k : id) {
            if (!ret.containsKey(k)) {
                ret.put(k, null);
            }
        }
        return ret;
    }

    @Nullable
    @Override
    public V findFirstByCriteria(Criterion criterion, Order order) {
        final List<V> ret = findByCriteria(criterion, null, null, Collections.singletonList(order), null, null);
        return ret == null || ret.isEmpty() ? null : ret.get(0);
    }

    @NotNull
    @Override
    public CompletableFuture<List<V>> findByCriteriaAsync(final Criterion criterion) {
        return CompletableFuture.supplyAsync(() -> findByCriteria(criterion), executor);
    }

    @NotNull
    @Override
    public CompletableFuture<V> findFirstByCriteriaAsync(final Criterion criterion, final Order order) {
        return CompletableFuture.supplyAsync(() -> findFirstByCriteria(criterion, order), executor);
    }

    @NotNull
    @Override
    public AbstractDao<K, V> with(final SessionFactory sessionFactory) {
        return new AbstractDaoImpl<>(sessionFactory, this.entityClass, this.executor);
    }

    @Override
    public void evict(K id) {
        this.sessionFactory.getCache().evictEntity(entityClass, id);
    }

    @Override
    public Class<V> getEntityClass() {
        return this.entityClass;
    }

}