package org.everthrift.utils.beans;

import org.apache.thrift.TBase;
import org.apache.thrift.TFieldIdEnum;
import sun.reflect.MethodAccessor;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

class ReadThriftMethodAccessor extends ThriftMethodAccessor {

    static TFieldIdEnum patch(Class entityClass, String propertyName, Method readMethod) throws IllegalAccessException, IllegalArgumentException, InvocationTargetException {

        final sun.reflect.MethodAccessor old = (MethodAccessor) getMethodAccessor.invoke(readMethod);
        if (old != null && old instanceof ThriftMethodAccessor) {
            return ((ThriftMethodAccessor) old).fId;
        }

        try {
            final ReadThriftMethodAccessor methodAccessor = new ReadThriftMethodAccessor(entityClass, propertyName);
            setMethodAccessor.invoke(readMethod, methodAccessor);
            return methodAccessor.fId;
        } catch (NotThriftProperty e) {
            return null;
        }
    }

    private ReadThriftMethodAccessor(Class entityClass, String propertyName) throws NotThriftProperty {
        super(entityClass, propertyName);
    }

    @Override
    public Object invoke(Object arg0, Object[] arg1) throws IllegalArgumentException, InvocationTargetException {

        if (arg1.length != 0) {
            throw new IllegalArgumentException();
        }

        if (((TBase) arg0).isSet(fId)) {
            return ((TBase) arg0).getFieldValue(fId);
        }

        return null;
    }
}
