package com.knockchat.appserver.model;

import java.util.Collection;

import com.google.common.base.Function;

public abstract class LazyLoaderHelper<K, V> {


    private final Function<Iterable<? extends K>, Integer> loadFunction;

    public LazyLoaderHelper() {

        loadFunction = new Function<Iterable<? extends K>, Integer>() {

            @Override
            public Integer apply(Iterable<? extends K> input) {
            	
            	if (input instanceof Collection && ((Collection)input).size() == 0)
            		return 0;

                return loadImpl(input);
            }
        };
    }
    
    protected abstract int loadImpl(Iterable<? extends K> input);
    protected abstract void loadImpl(K input);
    
    protected boolean beforeLoad(K key){
    	return true;
    }

	public boolean load(K key){
		
		if (!beforeLoad(key))
			return false;
		
        if (LazyLoadManager.addToLoad((Function) loadFunction, key)) {
            return false;
        }

        loadImpl(key);
        return true;		
	}
	
}
