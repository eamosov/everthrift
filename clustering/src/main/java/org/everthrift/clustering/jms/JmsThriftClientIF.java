package org.everthrift.clustering.jms;

public interface JmsThriftClientIF {
	
	public default <T> T on(Class<T> cls){
		return onIface(cls);
	}
	
	public <T> T onIface(Class<T> cls);
	
}
