package org.everthrift.appserver.model.lazy;

import com.google.common.util.concurrent.ListenableFuture;

public interface Registry {

    <K> boolean add(LazyLoader<K> l, K e, Object eq);
    <K> boolean add(LazyLoader<K> l, K e);

    void clear();

    ListenableFuture<Integer> load();

    Object[] getArgs();
}
