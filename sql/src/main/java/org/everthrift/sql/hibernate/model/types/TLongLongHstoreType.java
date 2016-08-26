package org.everthrift.sql.hibernate.model.types;

import gnu.trove.decorator.TLongLongMapDecorator;
import gnu.trove.map.hash.TLongLongHashMap;
import org.hibernate.HibernateException;

import java.util.Map;
import java.util.Map.Entry;

public class TLongLongHstoreType extends Trove4jHstoreType<TLongLongHashMap> {

    @SuppressWarnings("rawtypes")
    @Override
    public Class returnedClass() {
        return TLongLongHashMap.class;
    }

    @Override
    public Object deepCopy(Object value) throws HibernateException {
        return value == null ? null : new TLongLongHashMap((TLongLongHashMap) value);
    }

    @Override
    protected TLongLongHashMap transform(Map<String, String> input) {
        final TLongLongHashMap ret = new TLongLongHashMap();

        for (Entry<String, String> e : input.entrySet()) {
            ret.put(Long.parseLong(e.getKey()), Long.parseLong(e.getValue()));
        }

        return ret;
    }

    @SuppressWarnings("rawtypes")
    @Override
    protected Map transformReverse(TLongLongHashMap input) {
        return new TLongLongMapDecorator(input);
    }

}
