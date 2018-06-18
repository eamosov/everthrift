package org.everthrift.utils;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import org.springframework.beans.BeanUtils;

import java.beans.PropertyDescriptor;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.RandomAccess;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;


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
        final PropertyDescriptor[] pds = BeanUtils.getPropertyDescriptors(cls);
        final Map<String, PropertyDescriptor> m = Maps.newHashMapWithExpectedSize(pds.length);
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


    private static final AtomicReference<Map<Class, Constructor>> cache = new AtomicReference<Map<Class, Constructor>>(Maps.newIdentityHashMap());

    @SuppressWarnings({"rawtypes", "unchecked"})
    public static Object deepCopy(Object value) {

        if (value == Collections.EMPTY_LIST) {
            return value;
        } else if (value instanceof List) {
            final List _l = (List) value;

            if (_l.size() == 0) {
                return Lists.newArrayList();
            }

            final List ret = Lists.newArrayListWithCapacity(_l.size());
            if (_l instanceof RandomAccess) {
                for (int i = 0; i < _l.size(); i++) {
                    ret.add(copy(_l.get(i)));
                }
            } else {
                for (Object input : _l) {
                    ret.add(copy(input));
                }
            }
            return ret;
        } else if (value instanceof Set) {
            final Set _s = (Set) value;

            if (_s.size() == 0) {
                return Sets.newHashSet();
            }

            final Set ret = Sets.newHashSetWithExpectedSize(_s.size());

            for (Object input : _s) {
                ret.add(copy(input));
            }

            return ret;
        } else if (value instanceof Map) {
            final Map _m = (Map) value;
            if (_m.size() == 0) {
                return Maps.newHashMap();
            }

            final Map ret = Maps.newHashMapWithExpectedSize(_m.size());

            for (final Map.Entry e : (Set<Map.Entry>) (_m.entrySet())) {
                ret.put(copy(e.getKey()), copy(e.getValue()));
            }
            return ret;
        } else if (value instanceof Multimap) {
            final Multimap _m = (Multimap) value;

            if (_m.size() == 0) {
                return ArrayListMultimap.create();
            }

            final Multimap ret = ArrayListMultimap.create(_m.keySet().size(), _m.size() / _m.keySet().size() + 1);
            for (final Map.Entry e : (Set<Map.Entry>) (_m.entries())) {
                ret.put(copy(e.getKey()), copy(e.getValue()));
            }
            return ret;
        } else {
            return copy(value);
        }
    }

    private static Object copy(Object value) {

        if (value == null || value instanceof Number || value instanceof String || value instanceof Boolean) {
            return value;
        }

        final Map<Class, Constructor> _cache = cache.get();
        Constructor c = _cache.get(value.getClass());
        if (c == null) {
            try {
                c = value.getClass().getConstructor(value.getClass());
                final Map<Class, Constructor> __cache = Maps.newIdentityHashMap();
                __cache.putAll(_cache);
                __cache.put(value.getClass(), c);
                cache.set(__cache);
            } catch (final Exception e) {
                throw new RuntimeException(e);
            }
        }

        try {
            return c.newInstance(value);
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

}
