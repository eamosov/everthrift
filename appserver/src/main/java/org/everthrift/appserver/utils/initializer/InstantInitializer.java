package org.everthrift.appserver.utils.initializer;


public class InstantInitializer<T> implements Initializer<T> {

	private final T value;
	
	public InstantInitializer(T value) {
		this.value = value;
	}

	@Override
	public T get() {		
		return value;
	}

}
