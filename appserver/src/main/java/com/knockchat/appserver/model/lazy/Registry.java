package com.knockchat.appserver.model.lazy;


public interface Registry {	
	
	 <K> boolean add(LazyLoader<K> l, K e);
	 
	 void clear();
	 
	 int load();
}
