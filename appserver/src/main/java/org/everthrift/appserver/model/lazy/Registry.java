package org.everthrift.appserver.model.lazy;


public interface Registry {	
	
	 <K> boolean add(LazyLoader<K> l, K e);
	 
	 void clear();
	 
	 int load();
	 
	 Object[] getArgs();
}
