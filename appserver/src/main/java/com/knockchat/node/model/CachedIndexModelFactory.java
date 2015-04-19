package com.knockchat.node.model;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;

import net.sf.ehcache.Cache;
import net.sf.ehcache.loader.CacheLoader;

import org.apache.commons.lang.NotImplementedException;

import com.google.common.base.Function;
import com.knockchat.hibernate.dao.DummyEntity;

public abstract class CachedIndexModelFactory<K,V> extends AbstractCachedModelFactory<K, List<V>, Void, Serializable, DummyEntity> {

	public CachedIndexModelFactory(String cacheName) {
		super(cacheName);
	}

	public CachedIndexModelFactory(Cache cache) {
		super(cache);
	}

	@Override
	public final DummyEntity updateEntity(DummyEntity e) {
		throw new NotImplementedException();
	}

	@Override
	public final void deleteEntity(DummyEntity e){
		throw new NotImplementedException();
	}
	
	@Override
	protected CacheLoader getCacheLoader() {
		return new AbstractCacheListLoader<K, V, Object[]>(cacheName + "Loader",
				new Function<Object[], K>(){
					@Override
					public K apply(Object[] input) {
						return (K)input[1];
					}
				},
				new Function<Object[], V>(){
					@Override
					public V apply(Object[] input) {
						return (V)input[0];
					}
				}){

			@Override
			protected Collection<Object[]> loadImpl(Collection<K> keys) {
				return CachedIndexModelFactory.this.loadImpl(keys);
			}};
	}			

	/**
	 * 
	 * @param keys
	 * @return Object[] = Object[]{value, key}
	 */
	abstract protected Collection<Object[]> loadImpl(Collection<K> keys);
}
