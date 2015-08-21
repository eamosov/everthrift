package com.knockchat.appserver.model.lazy;

import it.unimi.dsi.fastutil.objects.ReferenceOpenHashSet;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ArrayListMultimap;

public class RegistryImpl implements Registry {
	
	private static final Logger log = LoggerFactory.getLogger(RegistryImpl.class);
	
	private final ArrayListMultimap<LazyLoader<?>, Object> loadList = ArrayListMultimap.create();
	private final Set<Object> uniqSet = new ReferenceOpenHashSet<Object>();

	public RegistryImpl() {
	}

	@Override
	public <K> boolean add(LazyLoader<K> l, K e) {
		
		if (uniqSet.add(e)){
			loadList.put(l, e);
			return true;
		}else{
			log.debug("skip duplicated: {}", e);
			return false;
		}			
	}
	
	@Override
	public void clear(){
		loadList.clear();
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public int load(){
		
		if (loadList.isEmpty())
			return 0;
		
		int nLoaded = 0;
		for (Map.Entry<LazyLoader<?>, Collection<Object>> e : loadList.asMap().entrySet()){
			final List<Object> entities = (List)e.getValue();
			if (!entities.isEmpty()){
				nLoaded += ((LazyLoader)e.getKey()).process(entities);
			}
		}
		return nLoaded;
	}

}
