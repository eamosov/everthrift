package com.knockchat.node.model;

import java.util.Collection;
import java.util.Map;

public interface MultiLoader<K,V> {

	public Map<K, V> findByIds(Collection<K> ids);
}
