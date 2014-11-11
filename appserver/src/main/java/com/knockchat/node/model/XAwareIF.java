package com.knockchat.node.model;


public interface XAwareIF<K,V> {

	 public boolean isSetId();
	 public void set(V o);
	 public K getId();
	
}
