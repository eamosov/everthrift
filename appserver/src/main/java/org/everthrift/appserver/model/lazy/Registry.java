package org.everthrift.appserver.model.lazy;

import java.util.concurrent.CompletableFuture;

public interface Registry {

    //уникальность контролируется uniqueKey
    <K> boolean addWithUnique(LazyLoader<K> l, K e, Object uniqueKey);

    //уникальность контролируется парой (e,eq)
    default <K> boolean add(LazyLoader<K> l, K e, Object eq){
        return addWithUnique(l, e, new UniqKey(e, eq));
    }

    default <K> boolean add(LazyLoader<K> l, K e){
        return add(l, e, null);
    }

    void clear();

    CompletableFuture<Integer> load();

    Object[] getArgs();
}
