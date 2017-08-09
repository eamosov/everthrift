package org.everthrift.sql.pgsql;

import com.google.common.collect.Lists;
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
import org.hibernate.CacheMode;
import org.hibernate.SessionFactory;
import org.hibernate.criterion.Restrictions;
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

    private final SessionFactory sessionFactory;

    private final ListeningExecutorService listeningExecutorService;

    protected final LocalEventBus localEventBus;

    protected final AbstractDaoImpl<PK, ENTITY> dao;

    protected final Class<ENTITY> entityClass;


    private final String ALL_KEYS = "__all__keys__";

    @Override
    public E createNotFoundException(PK id) {
        return (E) new TException("Entity with PK '" + id + "' not found");
    }

    protected AbstractPgSqlModelFactory(Cache cache, Class<ENTITY> entityClass,
                                        @Qualifier("listeningCallerRunsBoundQueueExecutor") ListeningExecutorService listeningExecutorService,
                                        SessionFactory sessionFactory,
                                        LocalEventBus localEventBus) {
        super(cache);

        this.entityClass = entityClass;
        dao = new AbstractDaoImpl<>(this.entityClass);
        if (cache != null) {
            dao.setCacheMode(CacheMode.IGNORE);
        }

        this.listeningExecutorService = listeningExecutorService;
        this.localEventBus = localEventBus;
        this.sessionFactory = sessionFactory;
        _afterPropertiesSet();
    }

    protected AbstractPgSqlModelFactory(String cacheName, Class<ENTITY> entityClass,
                                        @Qualifier("listeningCallerRunsBoundQueueExecutor") ListeningExecutorService listeningExecutorService,
                                        SessionFactory sessionFactory,
                                        LocalEventBus localEventBus) {
        super(cacheName);

        this.entityClass = entityClass;
        dao = new AbstractDaoImpl<>(this.entityClass);

        if (cacheName != null) {
            dao.setCacheMode(CacheMode.IGNORE);
        }

        this.listeningExecutorService = listeningExecutorService;
        this.localEventBus = localEventBus;
        this.sessionFactory = sessionFactory;
        _afterPropertiesSet();
    }

    private void _afterPropertiesSet() {

        dao.setSessionFactory(sessionFactory);
        dao.setExecutor(listeningExecutorService);

        localEventBus.register(this);
    }

    @PostConstruct
    private void afterPropertiesSet() {
        _afterPropertiesSet();
    }

    protected final void _invalidateEhCache(PK id, InvalidateCause invalidateCause) {
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

    @Override
    protected ENTITY fetchEntityById(PK id) {
        return dao.findById(id);
    }

    @Override
    protected Map<PK, ENTITY> fetchEntityByIdAsMap(Collection<PK> ids) {

        if (getCache() == null) {
            log.warn("fetch by collection, but cache is null");
        }

        return dao.findByIdsAsMap(ids);
    }

    public final AbstractDao<PK, ENTITY> getDao() {
        return dao;
    }

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
                for (ENTITY entity : entities) {
                    keys.add(entity.getPk());
                    cache.put(new Element(entity.getPk(), entity), true);
                }
                cache.put(new Element(ALL_KEYS, keys), true);
                return entities;
            } else {
                return findEntityByIdsInOrder((List) cachedKeys.getObjectValue());
            }
        }
    }

    public void fetchAll(final int batchSize, Consumer<List<ENTITY>> consumer) {
        //TODO fetchAll можно переделать на курсор
        final List<ENTITY> entities = listAll();
        for (List<ENTITY> batch : Lists.partition(entities, batchSize)) {
            consumer.accept(batch);
        }
    }

}
