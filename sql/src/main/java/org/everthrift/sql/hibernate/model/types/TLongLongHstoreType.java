package org.everthrift.sql.hibernate.model.types;

import gnu.trove.decorator.TLongLongMapDecorator;
import gnu.trove.map.hash.TLongLongHashMap;
import org.hibernate.HibernateException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Map;
import java.util.Map.Entry;

public class TLongLongHstoreType extends Trove4jHstoreType<TLongLongHashMap> {

    @NotNull
    @SuppressWarnings("rawtypes")
    @Override
    public Class returnedClass() {
        return TLongLongHashMap.class;
    }

    @Nullable
    @Override
    public Object deepCopy(@Nullable Object value) throws HibernateException {
        return value == null ? null : new TLongLongHashMap((TLongLongHashMap) value);
    }

    @NotNull
    @Override
    protected TLongLongHashMap transform(@NotNull Map<String, String> input) {
        final TLongLongHashMap ret = new TLongLongHashMap();

        for (Entry<String, String> e : input.entrySet()) {
            ret.put(Long.parseLong(e.getKey()), Long.parseLong(e.getValue()));
        }

        return ret;
    }

    @NotNull
    @SuppressWarnings("rawtypes")
    @Override
    protected Map transformReverse(TLongLongHashMap input) {
        return new TLongLongMapDecorator(input);
    }

}
