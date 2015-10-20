package com.knockchat.utils;


import java.lang.reflect.Field;
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
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import com.google.gson.reflect.TypeToken;
import com.knockchat.utils.meta.MetaClass;
import com.knockchat.utils.meta.MetaClasses;
import com.knockchat.utils.meta.MetaProperty;
import com.knockchat.utils.thrift.TBaseHasModel;
import com.knockchat.utils.thrift.Utils;

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

            if (!(src instanceof TBase)) {
                log.error("coudn't serialize class {}", src.getClass());
                return new JsonObject();
            }

            final JsonObject jo = new JsonObject();
            
            final Map<? extends TFieldIdEnum, FieldMetaData> map = Utils.getRootThriftClass(src.getClass()).second;
            
            if (map == null) {
                log.error("coudn't serialize class {}", src.getClass());
                return new JsonObject();
            }
            
            for (TFieldIdEnum f : map.keySet()) {
            	
                if (src.isSet(f)) {

                    final Object v = src.getFieldValue(f);

                    if (v instanceof TBase) {
                        jo.add(f.getFieldName(), context.serialize(v/*, TBase.class*/));
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

		@Override
		public T deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) throws JsonParseException {
			
			if (!TBase.class.isAssignableFrom((Class)typeOfT))
				throw new JsonParseException("can not deserialize class " +  typeOfT.getClass().getSimpleName());
			
			final TBase o;
			try {
				final Class m = TBaseHasModel.getModel((Class)typeOfT);
				if (m == null)
					o = (TBase)((Class)typeOfT).newInstance();
				else
					o = (TBase)m.newInstance();
			} catch (InstantiationException | IllegalAccessException e1) {
				throw new JsonParseException(e1);
			}
						
			final MetaClass mc = MetaClasses.get((Class)typeOfT);
			if (mc == null)
				throw new JsonParseException("can not create MetaClass for class " + typeOfT.getClass().getSimpleName());
			
			for (Map.Entry<String, JsonElement> e:json.getAsJsonObject().entrySet()){
				final MetaProperty p = mc.getProperty(e.getKey());
				if (p == null){
					log.warn("coudn't find property {} for class {}, json={}", e.getKey(), mc.getName(), json.toString());
					continue;
				}
				
				final Field f;
				try {
					f = ClassUtils.getDeclaredField((Class)typeOfT, e.getKey());
				} catch (SecurityException | NoSuchFieldException e1) {
					throw new JsonParseException("class " + ((Class)typeOfT).getSimpleName(), e1);
				}
								
				p.set(o, context.deserialize(e.getValue(), f.getGenericType()));				
			}
						
			return (T)o;
		}

    }

    private static final Gson gson = new GsonBuilder().setPrettyPrinting().registerTypeHierarchyAdapter(TBase.class, new TBaseSerializer()).create();

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