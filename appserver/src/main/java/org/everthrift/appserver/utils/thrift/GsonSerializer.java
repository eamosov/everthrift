package org.everthrift.appserver.utils.thrift;

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

import java.beans.PropertyDescriptor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Type;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * @author efreet (Amosov Evgeniy)
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class GsonSerializer {

    public static class TBaseSerializer<T extends TBase> implements JsonSerializer<T>, JsonDeserializer<T> {

        final public static Type tBaseCollection = new TypeToken<Collection<TBase>>() {
        }.getType();

        private final static Logger log = LoggerFactory.getLogger(TBaseSerializer.class);

        @Override
        public JsonElement serialize(T src, Type typeOfSrc, JsonSerializationContext context) {
            return serialize(src, typeOfSrc, context, Collections.emptySet());
        }

        public JsonElement serialize(T src, Type typeOfSrc, JsonSerializationContext context, final Set<String> excludes) {

            if (!(src instanceof TBase)) {
                log.error("coudn't serialize class {}", src.getClass());
                return new JsonObject();
            }

            final JsonObject jo = new JsonObject();

            final Map<? extends TFieldIdEnum, FieldMetaData> map = ThriftUtils.getRootThriftClass(src.getClass()).second;

            if (map == null) {
                log.error("coudn't serialize class {}", src.getClass());
                return new JsonObject();
            }

            for (TFieldIdEnum f : map.keySet()) {

                if (src.isSet(f) && !excludes.contains(f.getFieldName())) {

                    final Object v = src.getFieldValue(f);

                    if (v instanceof TBase) {
                        jo.add(f.getFieldName(),
                               context.serialize(v/* , TBase.class */));
                    } else if (v instanceof Collection && !((Collection) v).isEmpty()
                        && ((Collection) v).iterator().next() instanceof TBase) {
                        jo.add(f.getFieldName(), context.serialize(v, tBaseCollection));
                    } else if (v instanceof String) {
                        jo.add(f.getFieldName(), context.serialize(((String) v).replace("%", "%25")));
                    } else {
                        jo.add(f.getFieldName(), context.serialize(v));
                    }
                }
            }

            return jo;
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

            final Map<String, PropertyDescriptor> pds = ClassUtils.getPropertyDescriptors(o.getClass());

            for (Map.Entry<String, JsonElement> e : json.getAsJsonObject().entrySet()) {

                final PropertyDescriptor pd = pds.get(e.getKey());

                if (pd == null || pd.getWriteMethod() == null) {
                    if (log.isDebugEnabled()) {
                        log.debug("coudn't find property {} for class {}, json={}", e.getKey(), o.getClass()
                                                                                                 .getSimpleName(),
                                  json.toString());
                    }
                    continue;
                }

                final Field f;
                try {
                    f = ClassUtils.getDeclaredField((Class) objCls, e.getKey());
                } catch (SecurityException | NoSuchFieldException e1) {
                    throw new JsonParseException("class " + ((Class) objCls).getSimpleName(), e1);
                }

                try {
                    pd.getWriteMethod().invoke(o, new Object[]{context.deserialize(e.getValue(), f.getGenericType())});
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