package org.everthrift.appserver.utils.thrift;

import com.google.common.base.Throwables;
import com.google.gson.FieldNamingPolicy;
import com.google.gson.FieldNamingStrategy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import com.google.gson.reflect.TypeToken;
import org.apache.thrift.TBase;
import org.apache.thrift.TFieldIdEnum;
import org.apache.thrift.meta_data.FieldMetaData;
import org.everthrift.thrift.TBaseHasModel;
import org.everthrift.utils.ClassUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeanUtils;

import java.beans.PropertyDescriptor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author efreet (Amosov Evgeniy)
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class GsonSerializer {

    public static class TBaseSerializer<T extends TBase> implements JsonSerializer<T>, JsonDeserializer<T> {

        private final FieldNamingStrategy namingStrategy;

        private final boolean allBeanProperties;

        final public static Type tBaseCollection = new TypeToken<Collection<TBase>>() {
        }.getType();

        private final static Logger log = LoggerFactory.getLogger(TBaseSerializer.class);

        public TBaseSerializer() {
            namingStrategy = FieldNamingPolicy.IDENTITY;
            allBeanProperties = false;
        }

        public TBaseSerializer(FieldNamingStrategy namingStrategy, boolean allBeanProperties) {
            this.namingStrategy = namingStrategy;
            this.allBeanProperties = allBeanProperties;
        }

        @Override
        public JsonElement serialize(T src, Type typeOfSrc, JsonSerializationContext context) {
            return serialize(src, typeOfSrc, context, Collections.emptySet());
        }

        private void serialize(JsonSerializationContext context, JsonObject jo, String fieldName, Object v) {
            if (v instanceof TBase) {

                jo.add(fieldName, context.serialize(v));

            } else if (v instanceof Collection && !((Collection) v).isEmpty() &&
                ((Collection) v).iterator().next() instanceof TBase) {

                jo.add(fieldName, context.serialize(v, tBaseCollection));

            } else if (v instanceof String) {

                jo.add(fieldName, context.serialize(((String) v).replace("%", "%25")));

            } else {

                jo.add(fieldName, context.serialize(v));
            }
        }

        private JsonElement serializeAll(T src, Type typeOfSrc, JsonSerializationContext context, final Set<String> excludes) {

            final JsonObject jo = new JsonObject();

            final Class classOfSrc = src.getClass();

            for (PropertyDescriptor pd : BeanUtils.getPropertyDescriptors(classOfSrc)) {

                if (pd.getWriteMethod() != null && pd.getReadMethod() != null) {

                    if (excludes.contains(pd.getName())) {
                        continue;
                    }

                    final String fieldName;

                    if (namingStrategy != FieldNamingPolicy.IDENTITY) {
                        final Field f;
                        try {
                            f = ClassUtils.getDeclaredField(classOfSrc, pd.getName());
                        } catch (NoSuchFieldException e) {
                            continue;
                        }

                        fieldName = namingStrategy.translateName(f);
                    } else {
                        fieldName = pd.getName();
                    }

                    final Object value;
                    try {
                        value = pd.getReadMethod().invoke(src);
                    } catch (IllegalAccessException | InvocationTargetException e) {
                        throw Throwables.propagate(e);
                    }

                    if (value == null) {
                        continue;
                    }

                    serialize(context, jo, fieldName, value);
                }
            }

            return jo;
        }

        private JsonElement serializeTBaseOnly(T src, Type typeOfSrc, JsonSerializationContext context, final Set<String> excludes) {

            if (!(src instanceof TBase)) {
                log.error("coudn't serialize class {}", src.getClass());
                return new JsonObject();
            }

            final JsonObject jo = new JsonObject();
            final Class<TBase> classOfSrc = (Class) src.getClass();

            final Map<? extends TFieldIdEnum, FieldMetaData> map = ThriftUtils.getRootThriftClass(classOfSrc).second;

            if (map == null) {
                log.error("coudn't serialize class {}", src.getClass());
                return new JsonObject();
            }

            final List<TFieldIdEnum> keys = new ArrayList<>(map.keySet());
            Collections.sort(keys, (a, b) -> a.getFieldName().compareTo(b.getFieldName()));

            for (TFieldIdEnum f : keys) {

                if (src.isSet(f) && !excludes.contains(f.getFieldName())) {

                    final String fieldName;

                    if (namingStrategy != FieldNamingPolicy.IDENTITY) {
                        try {
                            fieldName = namingStrategy.translateName(ClassUtils.getDeclaredField(classOfSrc, f.getFieldName()));
                        } catch (SecurityException | NoSuchFieldException e1) {
                            throw new JsonParseException("class " + ((Class) classOfSrc).getSimpleName(), e1);
                        }
                    } else {
                        fieldName = f.getFieldName();
                    }

                    serialize(context, jo, fieldName, src.getFieldValue(f));
                }
            }

            return jo;
        }

        public JsonElement serialize(T src, Type typeOfSrc, JsonSerializationContext context, final Set<String> excludes) {

            return allBeanProperties ? serializeAll(src, typeOfSrc, context, excludes) : serializeTBaseOnly(src, typeOfSrc, context, excludes);
        }

        @Override
        public T deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) throws JsonParseException {

            final Object o;
            Class objCls;
            try {

                if ((objCls = TBaseHasModel.getModel((Class) typeOfT)) == null) {
                    objCls = (Class) typeOfT;
                }

                o = objCls.newInstance();
            } catch (InstantiationException | IllegalAccessException e1) {
                throw new JsonParseException(e1);
            }

            final JsonObject jsonObject = json.getAsJsonObject();
            for (PropertyDescriptor pd : BeanUtils.getPropertyDescriptors(objCls)) {
                if (pd.getWriteMethod() == null) {
                    continue;
                }

                final Field f;
                final JsonElement value;

                if (namingStrategy == FieldNamingPolicy.IDENTITY) {
                    value = jsonObject.get(pd.getName());

                    if (value == null) {
                        continue;
                    }

                    try {
                        f = ClassUtils.getDeclaredField((Class) objCls, pd.getName());
                    } catch (SecurityException | NoSuchFieldException e1) {
                        throw new JsonParseException("class " + ((Class) objCls).getSimpleName(), e1);
                    }
                } else {

                    try {
                        f = ClassUtils.getDeclaredField((Class) objCls, pd.getName());
                    } catch (SecurityException | NoSuchFieldException e1) {
                        continue;
                    }

                    value = jsonObject.get(namingStrategy.translateName(f));

                    if (value == null) {
                        continue;
                    }
                }

                try {
                    pd.getWriteMethod().invoke(o, new Object[]{context.deserialize(value, f.getGenericType())});
                } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e1) {
                    throw new RuntimeException(e1);
                }
            }

            return (T) o;
        }

    }

    private static final Gson gson = new GsonBuilder().setPrettyPrinting()
                                                      .registerTypeHierarchyAdapter(TBase.class, new TBaseSerializer())
                                                      .create();

    public static Gson get() {
        return gson;
    }

    public static String toJson(TBase src) {
        return get().toJson(src, TBase.class);
    }

    public static <T extends TBase> String toJson(Collection<T> src) {
        return get().toJson(src, TBaseSerializer.tBaseCollection);
    }

    public static JsonElement toJsonTree(TBase src) {
        return get().toJsonTree(src, TBase.class);
    }
}