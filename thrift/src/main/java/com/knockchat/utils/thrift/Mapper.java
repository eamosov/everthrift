package com.knockchat.utils.thrift;

import java.lang.reflect.InvocationTargetException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.thrift.TBase;

public class Mapper {
	
	private static final Map<Class, Class> m = new ConcurrentHashMap<Class, Class>();

	public static <T extends TBase> void map(Class<T> src, Class<? extends T> dst){		
		m.put(src, dst);
	}

	public static <T extends TBase> T create(Class<T> src){		
		try {
			return from(src).newInstance();
		} catch (InstantiationException | IllegalAccessException e) {
			throw new RuntimeException(e);
		}
	}

	public static <T extends TBase> T create(Class<T> src, T arg){		
		try {
			return from(src).getConstructor(arg.getClass()).newInstance(arg);
		} catch (InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException | NoSuchMethodException | SecurityException e) {
			throw new RuntimeException(e);
		}
	}

	public static <T> Class<T> from(Class<T> src){
		return (Class)m.getOrDefault(src, src);
	}
}
