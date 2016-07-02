package org.everthrift.utils.initializer;

import java.io.Serializable;

public abstract class CachedInitializer<T> implements Initializer<T>, Serializable {

    private static final long serialVersionUID = 1L;

    private transient volatile T object;
    private transient volatile long t;
    private final long timeoutMillis;

    public CachedInitializer(long timeoutMillis){
        this.timeoutMillis = timeoutMillis;
    }

    @Override
    public T get() {

        T _object = object;
        long _t = t;
        final long now = System.currentTimeMillis();

        if (_object == null || (now - _t) > timeoutMillis ) {
            synchronized(this){
                _object = object;
                _t = t;
                if (_object == null || (now - _t) > timeoutMillis ) {
                    object = _object = initialize();
                    t = _t = now;
                }
                return _object;
            }
        }

        return _object;
    }

    protected abstract T initialize();

}
