package com.knockchat.node.model;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import net.sf.ehcache.CacheException;
import net.sf.ehcache.Ehcache;
import net.sf.ehcache.Status;
import net.sf.ehcache.loader.CacheLoader;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

@SuppressWarnings("rawtypes")
public abstract class AbstractCacheListLoader<K,V> implements CacheLoader {
	
	private final String name;
	private final Function<V, K> keyExtractor;

	public AbstractCacheListLoader(String name, Function<V, K> keyExtractor) {
		super();
		this.name = name;
		this.keyExtractor = keyExtractor;
	}
	
	@Override
	public Object load(Object key, Object argument) {
		return load(key);
	}

	@Override
	public Map loadAll(Collection keys, Object argument) {		    	
		return loadAll(keys);
	}
		
	protected abstract Collection<V> loadImpl(Collection<K> keys);
	protected abstract List<V> loadImpl(K key);
	
    private final Map<K, List<V>> loadAllImpl(Collection<K> keys) {
        final Map<K, List<V>> ret = Maps.newHashMap();
        final Collection<V> rs = loadImpl(keys);
        for (V r : rs) {
        	final K k = keyExtractor.apply(r);        	
        	List<V> l = ret.get(k);
        	if (l == null){
        		l = Lists.newArrayList();
        		ret.put(k, l);
        	}
        	l.add(r);
        }
        for (K k : keys)
            if (!ret.containsKey(k))
                ret.put(k, null);
        
        return ret;
    }
	
	@SuppressWarnings("unchecked")
	@Override
	public final Map loadAll(Collection keys) {
    	if (keys.size() == 0)
    		return Collections.emptyMap();
    	
    	if (keys.size() == 1){
    		final Object key = keys.iterator().next();
    		return Collections.singletonMap(key, load(key));
    	}

		return loadAllImpl(keys);		
	}

	@Override
	public String getName() {
		return name;
	}

	@Override
	public CacheLoader clone(Ehcache cache) throws CloneNotSupportedException {
		throw new CloneNotSupportedException();
	}

	@Override
	public void init() {
	}

	@Override
	public void dispose() throws CacheException {
	}

	@Override
	public Status getStatus() {
		return Status.STATUS_ALIVE;
	}

    @Override
    public final Object load(Object arg0) throws CacheException {
    	return loadImpl((K)arg0);
    }
}
