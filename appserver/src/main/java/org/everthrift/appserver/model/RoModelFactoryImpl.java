package org.everthrift.appserver.model;

import com.google.common.base.Predicates;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.everthrift.appserver.model.lazy.LazyLoader;
import org.everthrift.appserver.model.lazy.Registry;
import org.everthrift.appserver.model.lazy.UniqKey;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.RandomAccess;
import java.util.Set;
import java.util.SortedSet;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;

public abstract class RoModelFactoryImpl<PK, ENTITY, E extends Exception> implements RoModelFactoryIF<PK, ENTITY, E> {

    public RoModelFactoryImpl() {

    }

    protected interface MultiLoader<K, V> {

        @NotNull
        public Map<K, V> findByIds(Set<K> ids);
    }

    protected final Logger log = LoggerFactory.getLogger(this.getClass());

    @NotNull
    @Override
    final public Collection<ENTITY> findEntityById(@NotNull Set<PK> ids) {

        if (CollectionUtils.isEmpty(ids)) {
            return Collections.emptyList();
        }

        return Collections2.filter(findEntityByIdAsMap(ids).values(), Predicates.notNull());
    }

    @NotNull
    @Override
    final public List<ENTITY> findEntityByIdsInOrder(@NotNull List<PK> ids) {

        if (CollectionUtils.isEmpty(ids)) {
            return Collections.emptyList();
        }

        final List<ENTITY> ret = Lists.newArrayListWithExpectedSize(ids.size());
        final Map<PK, ENTITY> loaded = findEntityByIdAsMap(ImmutableSet.copyOf(ids));
        for (PK id : ids) {
            final ENTITY v = loaded.get(id);
            if (v != null) {
                ret.add(v);
            }
        }
        return ret;
    }

    @NotNull
    @Override
    public Map<List<PK>, List<ENTITY>> findEntityByCollectionIds(@NotNull Collection<List<PK>> listCollection) {

        if (CollectionUtils.isEmpty(listCollection)) {
            return Collections.emptyMap();
        }

        final Set<PK> totalIds = new HashSet<>(listCollection.size() * 2);
        for (Collection<PK> ids : listCollection) {
            totalIds.addAll(ids);
        }

        final Map<PK, ENTITY> intermediateResult = findEntityByIdAsMap(totalIds);
        final Map<List<PK>, List<ENTITY>> result = Maps.newHashMapWithExpectedSize(listCollection.size());

        for (List<PK> ids : listCollection) {
            List<ENTITY> places = Lists.newArrayListWithCapacity(ids.size());
            for (PK id : ids) {
                places.add(intermediateResult.get(id));
            }
            result.put(ids, places);
        }
        return result;
    }

    protected final LazyLoader<XAwareIF<PK, ENTITY>> lazyLoader = this::_load;

    protected final LazyLoader<XAwareIF<List<PK>, List<ENTITY>>> lazyListLoader = this::_loadList;

    public boolean lazyLoad(@NotNull Registry r, @NotNull XAwareIF<PK, ENTITY> m) {
        return m.isSetId() && r.add(lazyLoader, m);
    }

    public boolean lazyLoad(@NotNull Registry r, @NotNull XAwareIF<PK, ENTITY> m, Object entity, String propertyName) {
        return m.isSetId() && r.addWithUnique(lazyLoader, m, new UniqKey(entity, propertyName));
    }

    public boolean lazyLoad(@NotNull Registry r, @NotNull XAwareIF<PK, ENTITY> m, Object uniqueKey) {
        return m.isSetId() && r.addWithUnique(lazyLoader, m, uniqueKey);
    }

    public void lazyListLoad(@NotNull Registry r, @NotNull XAwareIF<List<PK>, List<ENTITY>> m) {
        if (m.isSetId()) {
            r.add(lazyListLoader, m);
        }
    }

    public void lazyListLoad(@NotNull Registry r, @NotNull XAwareIF<List<PK>, List<ENTITY>> m, Object uniqueKey) {
        if (m.isSetId()) {
            r.addWithUnique(lazyListLoader, m, uniqueKey);
        }
    }

    public void lazyListLoad(@NotNull Registry r, @NotNull XAwareIF<List<PK>, List<ENTITY>> m, Object entity, String propertyName) {
        if (m.isSetId()) {
            r.addWithUnique(lazyListLoader, m, new UniqKey(entity, propertyName));
        }
    }

    protected int _loadList(@NotNull Iterable<XAwareIF<List<PK>, List<ENTITY>>> s) {
        return RoModelFactoryImpl.joinByIds(s,
                                            input -> input.isSetId() ? input.getId() : null,
                                            (input1, input2) -> input1.set(input2.stream()
                                                                                 .filter(i -> i != null)
                                                                                 .collect(Collectors.toList())),
                                            RoModelFactoryImpl.this::findEntityByCollectionIds);
    }

    @SuppressWarnings("unchecked")
    protected int _load(@NotNull Iterable<? extends XAwareIF<PK, ENTITY>> s) {

        if (log.isDebugEnabled()) {
            int i = 0;
            for (Object j : s) {
                i++;
            }
            log.trace("loading {} entities", i);
        }

        return joinByIds(s,
                         input -> input.isSetId() ? input.getId() : null,
                         (input1, input2) -> {
                             if (input2 instanceof List) {
                                 input1.set((ENTITY) (((List) input2).stream()
                                                                     .filter(_i -> _i != null)
                                                                     .collect(Collectors.toList())));
                             } else if (input2 instanceof Set) {
                                 input1.set((ENTITY) Sets.filter((Set) input2, Predicates.notNull()));
                             } else if (input2 instanceof Collection) {
                                 input1.set((ENTITY) Collections2.filter((Set) input2, Predicates.notNull()));
                             } else {
                                 input1.set(input2);
                             }
                         });
    }

    public <T> int joinByIds(Iterable<? extends T> s, @NotNull Function<T, PK> getEntityId, @NotNull BiConsumer<T, ENTITY> setEntity) {
        return joinByIds(s, getEntityId, setEntity, this::findEntityByIdAsMap);
    }

    /**
     * K - тип ключа привязываемого объекта U - тип привязываемого объекта T -
     * тип объекта, к которому происходит привязка
     *
     * @param s           - список объектов, к которым добавить связь
     * @param getEntityId - функция получения ключа(K) связанного объекта
     * @param setEntity   - функция присвоения связи
     * @param loader      - загрузчик объектов для связи(U) по их ключам (K)
     */
    @SuppressWarnings("unchecked")
    public static <K, U, T> int joinByIds(@NotNull final Iterable<? extends T> s, @NotNull Function<T, K> getEntityId, @NotNull BiConsumer<T, U> setEntity,
                                          @NotNull MultiLoader<K, U> loader) {

        if (s instanceof Collection && ((Collection) s).size() == 0) {
            return 0;
        }

        final Set<K> ids = new HashSet<K>();

        if (s instanceof RandomAccess) {

            final List<? extends T> _s = (List) s;

            for (int i = 0; i < _s.size(); i++) {
                final K id = getEntityId.apply(_s.get(i));
                if (id != null) {
                    ids.add(id);
                }
            }

        } else {
            for (T i : s) {
                final K id = getEntityId.apply(i);
                if (id != null) {
                    ids.add(id);
                }
            }
        }

        final Map<K, U> loaded = loader.findByIds(ids);
        int k = 0;
        if (s instanceof RandomAccess) {
            final List<? extends T> _s = (List) s;
            for (int j = 0; j < _s.size(); j++) {
                final T i = _s.get(j);
                final K id = getEntityId.apply(i);
                if (id == null) {
                    continue;
                }

                final U u = loaded.get(id);
                if (u != null) {
                    k++;
                }
                setEntity.accept(i, u);
            }
        } else {
            for (T i : s) {
                final K id = getEntityId.apply(i);
                if (id == null) {
                    continue;
                }

                final U u = loaded.get(id);
                if (u != null) {
                    k++;
                }
                setEntity.accept(i, u);
            }
        }
        return k;
    }

}
