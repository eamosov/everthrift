package org.everthrift.appserver.model.lazy;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;
import com.jasongoodwin.monads.Try;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

public class RegistryImpl implements Registry {

    private static final Logger log = LoggerFactory.getLogger(RegistryImpl.class);

    private final ArrayListMultimap<LazyLoader<?>, Object> loadList = ArrayListMultimap.create();

    private final Set<Object> uniqSet = new HashSet<Object>();

    private Object[] args;

    public RegistryImpl() {

    }

    public RegistryImpl(Object[] args) {
        this.args = args;
    }

    @Override
    public synchronized <K> boolean addWithUnique(LazyLoader<K> l, K e, Object uniqueKey) {

        if (uniqSet.add(uniqueKey)) {
            loadList.put(l, e);
            return true;
        } else {
            log.debug("skip duplicated: {}", e);
            return false;
        }
    }

    @Override
    public synchronized void clear() {
        loadList.clear();
    }

    @Override
    @SuppressWarnings({"rawtypes", "unchecked"})
    public CompletableFuture<Integer> load() {

        int nLoaded = 0;
        final List<CompletableFuture<Integer>> asyncLoaders = Lists.newArrayList();

        synchronized (this) {
            if (loadList.isEmpty()) {
                return CompletableFuture.completedFuture(0);
            }

            for (Map.Entry<LazyLoader<?>, Collection<Object>> e : loadList.asMap().entrySet()) {
                final List<Object> entities = (List) e.getValue();
                if (!entities.isEmpty()) {

                    if (e.getKey() instanceof AsyncLazyLoader) {
                        asyncLoaders.add(((AsyncLazyLoader) e.getKey()).processAsync(entities));
                    } else {
                        nLoaded += ((LazyLoader) e.getKey()).process(entities);
                    }
                }
            }
        }

        if (asyncLoaders.isEmpty()) {
            return CompletableFuture.completedFuture(nLoaded);
        }

        final int _nLoaded = nLoaded;

        return CompletableFuture.allOf(asyncLoaders.toArray(new CompletableFuture[asyncLoaders.size()]))
                                .thenApply(V ->
                                               asyncLoaders
                                                   .stream()
                                                   .mapToInt(f -> Try.ofFailable(() -> f.get()).orElse(0))
                                                   .sum() + _nLoaded
                                );
    }

    @Override
    public Object[] getArgs() {
        return args;
    }

    public void setArgs(Object[] args) {
        this.args = args;
    }
}
