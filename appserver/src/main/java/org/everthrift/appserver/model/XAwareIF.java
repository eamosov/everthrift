package org.everthrift.appserver.model;

import org.jetbrains.annotations.NotNull;

import java.io.Serializable;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;

public interface XAwareIF<K, V> extends Serializable {

    public boolean isSetId();

    public void set(V o);

    public K getId();

    @NotNull
    static <K,V> XAwareIF<K, V> of(@NotNull final Supplier<Boolean> isSetId, @NotNull final Consumer<V> set, @NotNull final Supplier<K> getId){
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
