package org.everthrift.sql.pgsql;

import com.google.common.collect.Maps;
import gnu.trove.map.hash.TIntObjectHashMap;
import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheException;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Ehcache;
import net.sf.ehcache.Status;
import net.sf.ehcache.loader.CacheLoader;
import org.apache.commons.lang.NotImplementedException;
import org.apache.thrift.TException;
import org.everthrift.appserver.model.DaoEntityIF;
import org.everthrift.appserver.model.XAwareIF;
import org.everthrift.appserver.utils.ehcache.AbstractCacheEventListener;
import org.hibernate.criterion.Order;
import org.hibernate.criterion.Restrictions;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public abstract class DictionaryModelFactory<ENTITY extends DaoEntityIF> implements InitializingBean {

    @Nullable
    private PgSqlModelFactory<Integer, ENTITY, TException> factory;

    protected final String REMOVE_BY_ID;

    @NotNull
    protected final String KEY;

    protected final Order orderBy;

    private final String CACHE_NAME;

    private final Class<ENTITY> cls;

    public static class EntitiesHolder<ENTITY> implements Serializable {

        private static final long serialVersionUID = 1L;

        public final TIntObjectHashMap<ENTITY> map;

        public final List<ENTITY> list;

        public final Object custom;

        public EntitiesHolder(TIntObjectHashMap<ENTITY> map, List<ENTITY> list, Object custom) {
            super();

            this.map = map;
            this.list = list;
            this.custom = custom;
        }
    }

    @Nullable
    private volatile EntitiesHolder<ENTITY> entities;

    private final static long refreshInterval = 10 * 1000;

    private volatile long lastRefreshTs = System.currentTimeMillis();

    private Cache cache;

    @Autowired
    protected CacheManager cacheManager;

    @Nullable
    protected CacheLoader loader;

    protected DictionaryModelFactory(Class<ENTITY> cls, String tableName) {
        this(cls, tableName, Order.asc("id"));
    }

    protected DictionaryModelFactory(Class<ENTITY> cls, String tableName, Order orderBy) {

        this.REMOVE_BY_ID = String.format("DELETE FROM %s WHERE ID=?", tableName);
        this.KEY = tableName + "_models;";
        this.CACHE_NAME = tableName;
        this.cls = cls;
        this.orderBy = orderBy;
    }

    private boolean needRefresh(final long currentTs) {
        return currentTs - refreshInterval > this.lastRefreshTs;
    }

    public ENTITY findById(Integer id) {
        return getFullTable().get(id);
    }

    @NotNull
    public Map<Integer, ENTITY> findByIds(@NotNull Collection<Integer> ids) {
        final TIntObjectHashMap<ENTITY> map = getFullTable();
        final Map<Integer, ENTITY> ret = Maps.newHashMap();
        for (Integer i : ids) {
            ret.put(i, map.get(i));
        }

        return ret;
    }

    public List<ENTITY> getAll() {
        return _getFullTable().list;
    }

    @NotNull
    protected XAwareIF<Integer, ENTITY> getAwareAdapter(Void m) {
        throw new NotImplementedException();
    }

    public TIntObjectHashMap<ENTITY> getFullTable() {
        return _getFullTable().map;
    }

    @Nullable
    @SuppressWarnings("unchecked")
    protected EntitiesHolder<ENTITY> _getFullTable() {
        if (entities == null || needRefresh(System.currentTimeMillis())) {
            synchronized (this) {
                if (entities == null || needRefresh(System.currentTimeMillis())) {
                    entities = (EntitiesHolder<ENTITY>) cache.getWithLoader(KEY, loader, null).getObjectValue();
                    lastRefreshTs = System.currentTimeMillis();
                }
            }
        }
        return entities;
    }

    public boolean hasKey(int id) {
        return getFullTable().containsKey(id);
    }

    public int[] keys() {
        return getFullTable().keys();
    }

    public synchronized void invalidateCache() {
        cache.remove(KEY);
        entities = null;
    }

    @Nullable
    public ENTITY delete(Integer id) {
        if (!hasKey(id)) {
            return null;
        }

        final ENTITY result = findById(id);
        if (result != null) {
            factory.getDao().executeCustomUpdate(id, REMOVE_BY_ID, id);
            invalidateCache();
        }
        return result;
    }

    @NotNull
    public ENTITY update(@NotNull ENTITY entity) {
        final ENTITY result = factory.updateEntity(entity);
        invalidateCache();
        return result;
    }

    @Nullable
    protected EntitiesHolder<ENTITY> createEntitiesHolder(TIntObjectHashMap<ENTITY> map, List<ENTITY> list) {
        return new EntitiesHolder<ENTITY>(map, list, null);
    }

    @Override
    public void afterPropertiesSet() throws Exception {

        cache = cacheManager.getCache(CACHE_NAME);
        if (cache == null) {
            throw new RuntimeException("Can't find cache: " + CACHE_NAME);
        }

        factory = new PgSqlModelFactory<Integer, ENTITY, TException>(null, cls);

        loader = new CacheLoader() {

            @NotNull
            @Override
            public CacheLoader clone(Ehcache arg0) throws CloneNotSupportedException {
                throw new CloneNotSupportedException();
            }

            @Override
            public void dispose() throws CacheException {
            }

            @NotNull
            @Override
            public String getName() {
                return CACHE_NAME + "Loader";
            }

            @Override
            public Status getStatus() {
                return Status.STATUS_ALIVE;
            }

            @Override
            public void init() {
            }

            @Nullable
            @Override
            public Object load(Object arg0) throws CacheException {
                return load(arg0, null);
            }

            @Nullable
            @Override
            public Object load(Object arg0, Object arg1) {
                final List<ENTITY> list = factory.getDao().findByCriteria(Restrictions.sqlRestriction("true"),
                                                                          DictionaryModelFactory.this.orderBy);
                final TIntObjectHashMap<ENTITY> tmap = new TIntObjectHashMap<>(list.size());

                for (ENTITY e : list) {
                    tmap.put((Integer) e.getPk(), e);
                }

                return createEntitiesHolder(tmap, list);
            }

            @NotNull
            @Override
            public Map loadAll(Collection arg0) {
                throw new RuntimeException("not implemented");
            }

            @NotNull
            @Override
            public Map loadAll(Collection arg0, Object arg1) {
                throw new RuntimeException("not implemented");
            }
        };

        cache.getCacheEventNotificationService().registerListener(new AbstractCacheEventListener() {
            @Override
            public void notifyRemoveAll(Ehcache ehcache) {
                synchronized (DictionaryModelFactory.this) {
                    entities = null;
                }
            }
        });

    }
}
