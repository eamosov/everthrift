package com.knockchat.node.model;

import java.io.Serializable;


public interface XAwareIF<K,V> extends Serializable {

	 public boolean isSetId();
	 public void set(V o);
	 public K getId();
	
}
