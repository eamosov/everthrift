package com.knockchat.appserver.model.lazy;

import java.util.List;

public abstract class AbstractLazyLoader<K> implements LazyLoader<K> {


    public AbstractLazyLoader() {

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
