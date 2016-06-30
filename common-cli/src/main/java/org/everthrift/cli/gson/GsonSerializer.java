package org.everthrift.cli.gson;

import java.lang.reflect.Type;
import java.util.Collection;
import java.util.Map;

import org.apache.thrift.TBase;
import org.apache.thrift.TFieldIdEnum;
import org.apache.thrift.meta_data.FieldMetaData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import com.google.gson.reflect.TypeToken;

/**
 * @author efreet (Amosov Evgeniy)
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class GsonSerializer {

    public static class TBaseSerializer<T extends TBase> implements JsonSerializer<T> {

        final public static Type tBaseCollection = new TypeToken<Collection<TBase>>() {
        }.getType();

        private final static Logger log = LoggerFactory.getLogger(TBaseSerializer.class);

        @Override
        public JsonElement serialize(T src, Type typeOfSrc, JsonSerializationContext context) {

            if (!(src instanceof TBase)) {
                log.error("coudn't serialize class {}", src.getClass());
                return new JsonObject();
            }

            final JsonObject jo = new JsonObject();

            Map<? extends TFieldIdEnum, FieldMetaData> map = null;
            Class thriftClass = src.getClass();
            while(map == null && thriftClass !=null){
                try{
                    map = FieldMetaData.getStructMetaDataMap(thriftClass);
                }catch(Exception e){
                    map = null;
                }
                thriftClass = thriftClass.getSuperclass();
            }

            if (map == null) {
                log.error("coudn't serialize class {}", src.getClass());
                return new JsonObject();
            }

            for (TFieldIdEnum f : map.keySet()) {

                if (src.isSet(f)) {

                    final Object v = src.getFieldValue(f);

                    if (v instanceof TBase) {
                        jo.add(f.getFieldName(), context.serialize(v, TBase.class));
                    } else if (v instanceof Collection && !((Collection) v).isEmpty() && ((Collection) v).iterator().next() instanceof TBase) {
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

    }

    private static Gson gson = new GsonBuilder().setPrettyPrinting().registerTypeHierarchyAdapter(TBase.class, new TBaseSerializer()).create();

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