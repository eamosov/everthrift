package org.everthrift.utils.beans;

import org.apache.thrift.TBase;
import org.apache.thrift.TFieldIdEnum;
import org.apache.thrift.meta_data.FieldMetaData;
import org.apache.thrift.meta_data.FieldValueMetaData;
import org.everthrift.utils.Pair;

import java.lang.reflect.Method;
import java.util.Map;

abstract class ThriftMethodAccessor implements sun.reflect.MethodAccessor {
    protected final TFieldIdEnum fId;
    protected final FieldValueMetaData vmd;

    protected final static Method setMethodAccessor;
    protected final static Method getMethodAccessor;

    static class NotThriftProperty extends Exception {

    }

    static {
        try {
            setMethodAccessor = Method.class.getDeclaredMethod("setMethodAccessor", sun.reflect.MethodAccessor.class);
            setMethodAccessor.setAccessible(true);

            getMethodAccessor = Method.class.getDeclaredMethod("getMethodAccessor");
            getMethodAccessor.setAccessible(true);
        } catch (NoSuchMethodException | SecurityException e) {
            throw new RuntimeException(e);
        }
    }

    private static Pair<TFieldIdEnum, FieldValueMetaData> findFieldIdEnum(Class entityClass, String propertyName) {
        if (TBase.class.isAssignableFrom(entityClass)) {

            Map<TFieldIdEnum, FieldMetaData> map = null;
            Class<? extends TBase> thriftClass = (Class) entityClass;
            do {
                try {
                    map = (Map) FieldMetaData.getStructMetaDataMap(thriftClass);
                    if (map != null) {
                        break;
                    }
                } catch (Exception e) {
                    map = null;
                }
                thriftClass = (Class<? extends TBase>) thriftClass.getSuperclass();
            } while (thriftClass != null);

            if (map == null) {
                throw new RuntimeException("cound't final FieldMetaData for " + entityClass.getCanonicalName());
            }

            for (Map.Entry<TFieldIdEnum, FieldMetaData> e : map.entrySet()) {
                if (e.getKey().getFieldName().equalsIgnoreCase(propertyName)) {
                    return Pair.create(e.getKey(), e.getValue().valueMetaData);
                }
            }
        }

        return null;
    }

    ThriftMethodAccessor(Class entityClass, String propertyName) throws NotThriftProperty {
        final Pair<TFieldIdEnum, FieldValueMetaData> p = findFieldIdEnum(entityClass, propertyName);
        if (p == null) {
            throw new NotThriftProperty();
        }

        this.fId = p.first;
        this.vmd = p.second;
    }

}
