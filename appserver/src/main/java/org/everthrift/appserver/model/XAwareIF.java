package org.everthrift.appserver.model;

import java.io.Serializable;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;

public interface XAwareIF<K, V> extends Serializable {

    public boolean isSetId();

    public void set(V o);

    public K getId();

    static <K,V> XAwareIF<K, V> of(final Supplier<Boolean> isSetId, final Consumer<V> set, final Supplier<K> getId){
        return new XAwareIF<K, V>(){

            @Override
            public boolean isSetId() {
                return isSetId.get();
            }

            @Override
            public void set(V o) {
                set.accept(o);
            }

            @Override
            public K getId() {
                return getId.get();
            }
        };
    }

}
