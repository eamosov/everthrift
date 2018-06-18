package org.everthrift.appserver.model;

import org.everthrift.utils.ClassUtils;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.StorageType;
import org.infinispan.manager.EmbeddedCacheManager;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.CollectionUtils;

import javax.annotation.PostConstruct;
import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public abstract class AbstractCachedModelFactory<PK, ENTITY, E extends Exception> extends RoModelFactoryImpl<PK, ENTITY, E> {

    public enum NullValue implements Serializable {
        INSTANCE
    }

    @Autowired(required = false)
    private EmbeddedCacheManager cm;

    @Nullable
    private Cache<PK, Object> cache;

    protected final boolean copyOnRead;


    public enum InvalidateCause {
        INSERT,
        DELETE,
        UPDATE,
        UNKNOWN
    }

    /**
     * Конструктор создания фабрики не как Spring-bean
     */
    public AbstractCachedModelFactory(@Nullable Cache cache, boolean copyOnRead) {
        super();
        this.copyOnRead = copyOnRead;
        this.cache = cache;
        _afterPropertiesSet();
    }

    private void _afterPropertiesSet() {

        if (cache == null) {
            log.info("cache is disabled");
        }
    }

    @PostConstruct
    private void afterPropertiesSet() {
        _afterPropertiesSet();
    }

    public void invalidate(@NotNull PK id, @NotNull InvalidateCause invalidateCause) {
        if (cache != null) {
            if (log.isTraceEnabled()) {
                log.trace("invalidate {}/{}", getEntityClass().getSimpleName(), id);
            }
            cache.remove(id);
        }
    }

    public void invalidateLocal(@NotNull PK id, @NotNull InvalidateCause invalidateCause) {
        if (cache != null) {
            log.debug("invalidateLocal {}/{}", getEntityClass().getSimpleName(), id);
            cache.remove(id, true);
        }
    }

    public void invalidate(@NotNull Set<PK> ids, @NotNull InvalidateCause invalidateCause) {
        if (cache != null) {
            log.debug("invalidateLocal {}/{}", getEntityClass().getSimpleName(), ids);
            ids.forEach(cache::remove);
        }
    }

    @NotNull
    protected abstract Map<PK, ENTITY> fetchEntityByIdAsMap(@NotNull Set<PK> ids);

    @Nullable
    protected abstract ENTITY fetchEntityById(@NotNull PK id);

    private ENTITY copy(Object value) {

        if (cache != null && cache.getAdvancedCache()
                                  .getCacheConfiguration()
                                  .memory()
                                  .storageType() == StorageType.OBJECT && copyOnRead) {

            return (ENTITY) ClassUtils.deepCopy(value);
        } else {
            return (ENTITY) (copyOnRead ? ClassUtils.deepCopy(value) : value);
        }
    }

    @Override
    final public ENTITY findEntityById(@NotNull PK id) {

        if (id == null) {
            return null;
        }

        if (cache == null) {
            return fetchEntityById(id);
        }

        Object value;
        while ((value = cache.get(id)) == null) {
            final ENTITY fetched = fetchEntityById(id);
            cache.put(id, fetched == null ? NullValue.INSTANCE : fetched);
        }

        if (value == NullValue.INSTANCE) {
            return null;
        }

        return copy(value);
    }

    @NotNull
    @Override
    final public Map<PK, ENTITY> findEntityByIdAsMap(@NotNull Set<PK> ids) {
        if (CollectionUtils.isEmpty(ids)) {
            return Collections.emptyMap();
        }

        if (cache == null) {
            if (ids.size() == 1) {
                final PK id = ids.iterator().next();
                return Collections.singletonMap(id, fetchEntityById(id));
            } else {
                return fetchEntityByIdAsMap(ids);
            }
        }

        final Set<PK> toLoad = new HashSet<>(ids.size());
        toLoad.addAll(ids);

        final Map<PK, ENTITY> result = new HashMap<>();

        while (!toLoad.isEmpty()) {
            final Map<PK, Object> cached = cache.getAdvancedCache().getAll(toLoad);


            for (Map.Entry<PK, Object> e : cached.entrySet()) {
                if (e.getValue() == NullValue.INSTANCE) {
                    result.put(e.getKey(), null);
                } else {
                    result.put(e.getKey(), copy(e.getValue()));
                }
                toLoad.remove(e.getKey());
            }

            if (!toLoad.isEmpty()) {
                final Map<PK, ENTITY> loaded = fetchEntityByIdAsMap(toLoad);
                for (PK id : toLoad) {
                    final ENTITY fetched = loaded.get(id);
                    cache.put(id, fetched == null ? NullValue.INSTANCE : fetched);
                }
            }
        }

        return result;
    }

    @Nullable
    final public Cache<PK, Object> getCache() {
        return cache;
    }

    final public void setCache(@Nullable Cache<PK, ENTITY> cache) {
        this.cache = (Cache) cache;
        _afterPropertiesSet();
    }

    protected void setCreatedAt(@NotNull ENTITY e, long timestamp_mcs) {
        CreatedAtIF.setCreatedAt(e, timestamp_mcs / 1000);
    }

    protected void setUpdatedAt(@NotNull ENTITY e, long timestamp_mcs) {
        UpdatedAtIF.setUpdatedAt(e, timestamp_mcs / 1000);
    }

}
