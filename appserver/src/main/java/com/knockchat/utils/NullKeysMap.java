package com.knockchat.utils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

public class NullKeysMap<K, V> extends HashMap<K, Object> {

	public NullKeysMap(Iterable<K> keys){
		super();
		for (K k: keys){
			super.put(k, Collections.EMPTY_LIST);
		}
	}
	
	@Override
	public V put(K key, Object value) {
		List<V> v = (List)super.get(key);
		if (v == null || v == Collections.EMPTY_LIST){
			v = new ArrayList<V>();
			super.put(key, v);
		}
		v.add((V)value);
		return null;
	}
}
