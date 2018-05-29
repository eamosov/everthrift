package org.everthrift.clustering.rabbit;

public interface RabbitThriftClientIF {

    default <T> T on(Class<T> cls) {
        return onIface(cls);
    }

    <T> T onIface(Class<T> cls);

    default String getExchangeName(String serviceName){
        return serviceName;
    }
}
