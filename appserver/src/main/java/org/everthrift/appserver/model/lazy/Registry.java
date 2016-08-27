package org.everthrift.appserver.model.lazy;

import java.util.concurrent.CompletableFuture;

public interface Registry {

    <K> boolean add(LazyLoader<K> l, K e, Object eq);

    <K> boolean add(LazyLoader<K> l, K e);

    void clear();

    CompletableFuture<Integer> load();

    Object[] getArgs();
}
