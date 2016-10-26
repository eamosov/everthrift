package org.everthrift.utils;

import com.google.common.collect.Maps;
import org.springframework.beans.BeanUtils;

import java.beans.PropertyDescriptor;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.Map;


/**
 * @author efreet (Amosov Evgeniy)
 */
public class ClassUtils {

    public static boolean isBoxType(Class<?> type) {
        return type.equals(Byte.class) ||
            type.equals(Short.class) ||
            type.equals(Integer.class) ||
            type.equals(Long.class) ||
            type.equals(Float.class) ||
            type.equals(Double.class) ||
            type.equals(Boolean.class);
    }

    public static Class<?> getBoxType(Class<?> primitive) {
        if (primitive.equals(Byte.TYPE)) {
            return Byte.class;
        } else if (primitive.equals(Short.TYPE)) {
            return Short.class;
        } else if (primitive.equals(Integer.TYPE)) {
            return Integer.class;
        } else if (primitive.equals(Long.TYPE)) {
            return Double.class;
        } else if (primitive.equals(Float.TYPE)) {
            return Float.class;
        } else if (primitive.equals(Double.TYPE)) {
            return Double.class;
        } else if (primitive.equals(Boolean.TYPE)) {
            return Boolean.class;
        } else if (primitive.equals(Character.TYPE)) {
            return Character.class;
        } else if (!primitive.isPrimitive()) {
            throw new Error("Cann't get box for non-primitive type " + primitive.getName());
        } else {
            throw new Error("Unknown primitive type " + primitive.getName());
        }
    }

    public static byte[] writeObject(Object o) {
        final ByteArrayOutputStream ba = new ByteArrayOutputStream();
        final ObjectOutputStream os;
        try {
            os = new ObjectOutputStream(ba);
            os.writeObject(o);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        byte arr[] = ba.toByteArray();
        return arr;
    }

    public static Object readObject(byte[] b) throws ClassNotFoundException {
        return readObject(ByteBuffer.wrap(b));
    }

    public static Object readObject(ByteBuffer bb) throws ClassNotFoundException {
        final ByteBufferInputStream is = new ByteBufferInputStream(bb);

        try {
            final ObjectInputStream os = new ObjectInputStream(is);
            final Object co = (Object) os.readObject();
            return co;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static Map<String, PropertyDescriptor> getPropertyDescriptors(Class cls) {
        final Map<String, PropertyDescriptor> m = Maps.newHashMap();
        for (PropertyDescriptor d : BeanUtils.getPropertyDescriptors(cls)) {
            m.put(d.getName(), d);
        }
        return m;
    }

    @SuppressWarnings("rawtypes")
    public static Field getDeclaredField(Class cls, String fieldName) throws NoSuchFieldException, SecurityException {
        try {
            return cls.getDeclaredField(fieldName);
        } catch (NoSuchFieldException e) {
            final Class sc = cls.getSuperclass();
            if (sc != Object.class && sc != null) {
                return getDeclaredField(sc, fieldName);
            } else {
                throw e;
            }
        }
    }


}
