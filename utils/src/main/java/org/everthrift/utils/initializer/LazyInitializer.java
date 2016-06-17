package org.everthrift.utils.initializer;

import java.io.Serializable;


public abstract class LazyInitializer<T> implements Initializer<T>, Serializable {

	private static final long serialVersionUID = 1L;
	
	private transient T object;

    @Override
    public T get() {
    	
        if (object == null) {
        	object = initialize();
        }

        return object;
    }

    protected abstract T initialize();

}
