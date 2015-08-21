package com.knockchat.appserver.model;

import java.util.List;

public abstract class LazyLoaderHelper<K> implements LazyLoader<K> {


    public LazyLoaderHelper() {

    }
    
    @Override
    public int process(List<K> entities){
    	if (entities.isEmpty())
    		return 0;

        return loadImpl(entities);
    }
    
    protected abstract int loadImpl(List<K> entities);
    
    protected boolean beforeLoad(K key){
    	return true;
    }

	public boolean load(Registry r, K key){
		
		if (!beforeLoad(key))
			return false;
		
		return r.add(this, key);
	}
	
}
