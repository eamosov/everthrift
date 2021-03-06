package org.everthrift.appserver.model;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.infinispan.Cache;
import org.jetbrains.annotations.NotNull;
import org.springframework.util.CollectionUtils;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

public abstract class CachedIndexModelFactory<PK, ENTITY, E extends Exception> extends AbstractCachedModelFactory<PK, List<ENTITY>, E>
    implements RoModelFactoryIF<PK, List<ENTITY>, E> {

    /**
     * @param keys
     * @return Object[] = Object[]{value, key}
     */
    protected abstract Collection<Object[]> loadImpl(Collection<PK> keys);

    public CachedIndexModelFactory(Cache<PK, List<ENTITY>> cache, boolean copyOnRead) {
        super(cache, copyOnRead);
    }

    @Override
    public List<ENTITY> fetchEntityById(@NotNull PK id) {
        final Collection<Object[]> c = loadImpl(Collections.singleton(id));
        if (CollectionUtils.isEmpty(c)) {
            return Collections.emptyList();
        }

        final List<ENTITY> ret = Lists.newArrayList();
        for (Object[] o : c) {
            ret.add((ENTITY) o[0]);
        }

        return ret;
    }

    @NotNull
    @Override
    public Map<PK, List<ENTITY>> fetchEntityByIdAsMap(Set<PK> ids) {

        if (CollectionUtils.isEmpty(ids)) {
            return Collections.emptyMap();
        }

        final Collection<Object[]> c = loadImpl(ids);
        final Map<PK, List<ENTITY>> ret = Maps.newHashMapWithExpectedSize(ids.size());

        if (!CollectionUtils.isEmpty(c)) {
            for (final Object[] o : c) {
                final ENTITY v = (ENTITY) o[0];
                final PK k = (PK) o[1];
                List<ENTITY> vv = ret.get(k);
                if (vv == null) {
                    vv = Lists.newArrayList();
                    ret.put(k, vv);
                }
                vv.add(v);
            }
        }

        for (PK id : ids) {
            if (!ret.containsKey(id)) {
                ret.put(id, Collections.EMPTY_LIST);
            }
        }

        return ret;
    }
}
