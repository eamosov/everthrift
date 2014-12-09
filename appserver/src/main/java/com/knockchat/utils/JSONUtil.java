package com.knockchat.utils;

import java.util.Arrays;

import org.mozilla.javascript.NativeArray;
import org.mozilla.javascript.NativeJavaObject;
import org.mozilla.javascript.NativeObject;
import org.mozilla.javascript.ScriptableObject;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonSyntaxException;

public class JSONUtil {
	
	final Gson gson = new GsonBuilder().setPrettyPrinting().disableHtmlEscaping().create();
	
    public String SimpleNativeObjectToJson(NativeObject obj) {
    	
        return gson.toJson(simpleNativeObjectToJson(obj));
    }

    private JsonObject simpleNativeObjectToJson(NativeObject obj) {
    	
    	final JsonObject jo = new JsonObject();

        Object[] ids = obj.getIds();
        for (Object id : ids) {
            String key = id.toString();

            Object value = obj.get(key, obj);
            jo.add(key, valueToJson(value));
        }

        return jo;
    }

    private JsonElement valueToJson(Object value) {
    	
    	if (value instanceof NativeJavaObject){
    		return gson.toJsonTree(((NativeJavaObject) value).unwrap());
    		
    	}else if (value instanceof ScriptableObject && ((ScriptableObject) value).getClassName().equals("Date")) {
        	
            throw new JsonSyntaxException("Object has Complex values: " + value.toString() );
        } else if (value instanceof NativeArray) {
            return arrayToJson((NativeArray) value);
        } else if (value instanceof NativeObject) {
            return simpleNativeObjectToJson((NativeObject) value);
        } else if (value instanceof Double){
        	if ((double)((Double)value).longValue() == ((Double)value).doubleValue()) 
        		return gson.toJsonTree(((Double)value).longValue());
        	else
        		return gson.toJsonTree(value);
        }else{
        	return gson.toJsonTree(value);
        }
    }

    private JsonElement arrayToJson(NativeArray nativeArray){
    	
        Object[] propIds = nativeArray.getIds();
        if (isArray(propIds)) {
        	
        	final JsonArray ja = new JsonArray();

            for (Object propId : propIds) {
                Object value = nativeArray.get((Integer) propId, nativeArray);
                ja.add(valueToJson(value));
            }

            return ja;
        } else {

        	final JsonObject jo = new JsonObject();
        	
            for (Object propId : propIds) {
                Object value = nativeArray.get(propId.toString(), nativeArray);
                jo.add(propId.toString(), valueToJson(value));
            }

            return jo;
        }
    }

    private static boolean isArray(Object[] ids) {
        boolean result = true;
        for (Object id : ids) {
            if (!(id instanceof Integer)) {
                result = false;
                break;
            }
        }
        return result;
    }

    static String getNullableProperty(NativeObject obj, String... keys) {
        NativeObject result = obj;
        for (int i = 0; i < keys.length - 1; i++) {
            String key = keys[i];
            Object value = result.get(key, result);
            if (value instanceof NativeObject) {
                result = (NativeObject) value;
            } else {
                return null;
            }
        }
        return result.get(keys[keys.length - 1], result).toString();
    }

    static String getProperty(NativeObject obj, String... keys) {
        String property = getNullableProperty(obj, keys);
        if (property == null) {
            throw new RuntimeException("property missing in activity object : " + Arrays.toString(keys));
        } else {
            return property;
        }
    }
}
