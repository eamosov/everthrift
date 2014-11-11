package com.knockchat.utils;

import net.sf.ehcache.Cache;
import net.sf.ehcache.Element;

import com.google.common.base.Function;

public class CacheUtils {

	public static Object getWithLoader(Cache cache, Object key, Function<Object, Object> loader){
		final Element e = cache.get(key);
		if (e !=null)
			return e.getObjectValue();
		
		final Object value = loader.apply(key);
		cache.put(new Element(key, value));
		return value;
	}
}
