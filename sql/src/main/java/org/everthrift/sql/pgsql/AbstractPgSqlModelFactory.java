package org.everthrift.sql.pgsql;

import com.google.common.util.concurrent.ListeningExecutorService;
import net.sf.ehcache.Cache;
import net.sf.ehcache.Element;
import org.apache.thrift.TException;
import org.everthrift.appserver.model.AbstractCachedModelFactory;
import org.everthrift.appserver.model.DaoEntityIF;
import org.everthrift.appserver.model.LocalEventBus;
import org.everthrift.appserver.model.RwModelFactoryIF;
import org.everthrift.sql.hibernate.dao.AbstractDao;
import org.everthrift.sql.hibernate.dao.AbstractDaoImpl;
import org.hibernate.ScrollMode;
import org.hibernate.ScrollableResults;
import org.hibernate.SessionFactory;
import org.hibernate.StatelessSession;
import org.hibernate.criterion.Restrictions;
import org.hibernate.query.Query;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import javax.annotation.PostConstruct;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

public abstract class AbstractPgSqlModelFactory<PK extends Serializable, ENTITY extends DaoEntityIF, E extends TException>
    extends AbstractCachedModelFactory<PK, ENTITY, E> implements RwModelFactoryIF<PK, ENTITY, E> {

    @Autowired
    protected SessionFactory sessionFactory;

    @Autowired
    @Qualifier("listeningCallerRunsBoundQueueExecutor")
    private ListeningExecutorService listeningExecutorService;

    @Autowired
    protected LocalEventBus localEventBus;

    @NotNull
    protected final AbstractDaoImpl<PK, ENTITY> dao;

    protected final Class<ENTITY> entityClass;


    private final String ALL_KEYS = "__all__keys__";

    @NotNull
    @Override
    public E createNotFoundException(PK id) {
        return (E) new TException("Entity with PK '" + id + "' not found");
    }

    protected AbstractPgSqlModelFactory(Cache cache, Class<ENTITY> entityClass) {
        super(cache);

        this.entityClass = entityClass;
        dao = new AbstractDaoImpl<>(this.entityClass);
    }

    protected AbstractPgSqlModelFactory(String cacheName, Class<ENTITY> entityClass) {
        super(cacheName);

        this.entityClass = entityClass;
        dao = new AbstractDaoImpl<>(this.entityClass);
    }

    @PostConstruct
    private void afterPropertiesSet() {
        dao.setSessionFactory(sessionFactory);
        dao.setExecutor(listeningExecutorService);
        localEventBus.register(this);
    }

    protected final void _invalidateEhCache(@NotNull PK id, @NotNull InvalidateCause invalidateCause) {
        super.invalidate(id, invalidateCause);
        if (getCache() != null &&
            (invalidateCause == InvalidateCause.UNKNOWN ||
                invalidateCause == InvalidateCause.DELETE ||
                invalidateCause == InvalidateCause.INSERT)) {
            getCache().remove(ALL_KEYS);
        }
    }

    @Override
    public final void invalidate(PK id, InvalidateCause invalidateCause) {
        _invalidateEhCache(id, invalidateCause);
        getDao().evict(id);
    }

    @Override
    public final void invalidateLocal(PK id, InvalidateCause invalidateCause) {
        super.invalidateLocal(id, invalidateCause);
        getDao().evict(id);
    }

    @Nullable
    @Override
    protected ENTITY fetchEntityById(PK id) {
        return dao.findById(id);
    }

    @NotNull
    @Override
    protected Map<PK, ENTITY> fetchEntityByIdAsMap(Collection<PK> ids) {

        if (getCache() == null) {
            log.warn("fetch by collection, but cache is null");
        }

        return dao.findByIdsAsMap(ids);
    }

    @NotNull
    public final AbstractDao<PK, ENTITY> getDao() {
        return dao;
    }

    @NotNull
    @Override
    public final Class<ENTITY> getEntityClass() {
        return this.entityClass;
    }

    public List<ENTITY> listAll() {
        if (getCache() == null) {
            return getDao().findByCriteria(Restrictions.sqlRestriction("true"), null);
        } else {
            final Cache cache = getCache();
            final Element cachedKeys = cache.get(ALL_KEYS);
            if (cachedKeys == null) {
                final List<ENTITY> entities = getDao().findByCriteria(Restrictions.sqlRestriction("true"), null);
                final List<Serializable> keys = new ArrayList<>();
                entities.forEach(entity -> {
                    keys.add(entity.getPk());
                    cache.put(new Element(entity.getPk(), entity), true);
                });
                cache.put(new Element(ALL_KEYS, keys), true);
                return entities;
            } else {
                return findEntityByIdsInOrder((List) cachedKeys.getObjectValue());
            }
        }
    }

    public long countAll() {
        try (final StatelessSession ss = sessionFactory.openStatelessSession()) {
            final Query<Long> query = ss.createQuery("SELECT COUNT(*) FROM " + entityClass.getSimpleName());
            query.setReadOnly(true);
            query.setCacheable(false);

            return query.list().get(0);
        }
    }

    public void fetchAll(final int batchSize, @NotNull Consumer<List<ENTITY>> consumer) {

        try (final StatelessSession ss = sessionFactory.openStatelessSession()) {
            final Query<ENTITY> query = ss.createQuery("SELECT xx FROM " + entityClass.getSimpleName() + " xx");
            query.setReadOnly(true);
            query.setFetchSize(batchSize);
            query.setCacheable(false);

            try (ScrollableResults results = query.scroll(ScrollMode.FORWARD_ONLY)) {

                final List<ENTITY> batch = new ArrayList<>(batchSize);

                while (results.next()) {
                    batch.add((ENTITY) results.get()[0]);

                    if (batch.size() >= batchSize) {
                        consumer.accept(batch);
                        batch.clear();
                    }
                }

                if (!batch.isEmpty()) {
                    consumer.accept(batch);
                }
            }
        }
    }

}
