package org.everthrift.clustering.rabbit;

public interface RabbitThriftClientIF {

    public default <T> T on(Class<T> cls) {
        return onIface(cls);
    }

    public <T> T onIface(Class<T> cls);

}
