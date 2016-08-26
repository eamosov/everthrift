package org.everthrift.utils.beans;

import org.apache.thrift.TBase;
import org.apache.thrift.protocol.TType;
import sun.reflect.MethodAccessor;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.Arrays;

class WriteThriftMethodAccessor extends ThriftMethodAccessor {

    private final Class fieldType;

    static void patch(Class entityClass, String propertyName, Method writeMethod) throws IllegalAccessException, IllegalArgumentException, InvocationTargetException {

        final sun.reflect.MethodAccessor old = (MethodAccessor) getMethodAccessor.invoke(writeMethod);
        if (old != null && old instanceof ThriftMethodAccessor) {
            return;
        }

        try {
            setMethodAccessor.invoke(writeMethod, new WriteThriftMethodAccessor(entityClass, propertyName, writeMethod));
        } catch (NotThriftProperty e) {
        }
    }

    private WriteThriftMethodAccessor(Class entityClass, String propertyName, Method writeMethod) throws NotThriftProperty {
        super(entityClass, propertyName);
        fieldType = writeMethod.getParameterTypes()[0];
    }

    private boolean isBox(Class box, Class primitive) {

        return (box.equals(Byte.class) && primitive.equals(Byte.TYPE)) ||
            (box.equals(Double.class) && primitive.equals(Double.TYPE)) ||
            (box.equals(Short.class) && primitive.equals(Short.TYPE)) ||
            (box.equals(Integer.class) && primitive.equals(Integer.TYPE)) ||
            (box.equals(Long.class) && primitive.equals(Long.TYPE));
    }

    @Override
    public Object invoke(Object entity, Object[] arg1) throws IllegalArgumentException, InvocationTargetException {

        if (arg1.length != 1) {
            throw new IllegalArgumentException();
        }

        Object value = arg1[0];

        if (value != null && !fieldType.isAssignableFrom(value.getClass()) && !isBox(value.getClass(), fieldType)) {

            if (value instanceof Number) {
                switch (vmd.type) {
                    case TType.BYTE:
                        value = ((Number) value).byteValue();
                        break;
                    case TType.DOUBLE:
                        value = ((Number) value).doubleValue();
                        break;
                    case TType.I16:
                        value = ((Number) value).shortValue();
                        break;
                    case TType.I32:
                        value = ((Number) value).intValue();
                        break;
                    case TType.I64:
                        value = ((Number) value).longValue();
                        break;
                }
            }
        } else if (value != null && value.getClass().equals(byte[].class) && fieldType.equals(byte[].class)) {
            //В thrift setFieldValue ожидает ByteBuffer для типа binary
            value = ByteBuffer.wrap(Arrays.copyOf((byte[]) value, ((byte[]) value).length));
        }

        ((TBase) entity).setFieldValue(fId, value);

        return null;
    }
}
