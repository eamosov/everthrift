package com.knockchat.node.model;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import net.sf.ehcache.CacheException;
import net.sf.ehcache.Ehcache;
import net.sf.ehcache.Status;
import net.sf.ehcache.loader.CacheLoader;

public abstract class AbstractCacheLoader implements CacheLoader {
	
	private final String name;

	public AbstractCacheLoader(String name) {
		super();
		this.name = name;
	}
	
	@Override
	public Object load(Object key, Object argument) {
		return load(key);
	}

	@Override
	public Map loadAll(Collection keys, Object argument) {		    	
		return loadAll(keys);
	}
		
	protected abstract Map loadAllImpl(Collection keys);
	
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

}
