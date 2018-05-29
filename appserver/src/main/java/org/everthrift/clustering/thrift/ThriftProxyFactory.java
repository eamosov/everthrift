package org.everthrift.clustering.thrift;

import org.everthrift.thrift.ThriftServicesDiscovery;

import java.lang.reflect.Proxy;

public class ThriftProxyFactory {

    /**
     * alias for onIfaceAsAsync
     *
     * @param cls
     * @return
     */
    public static <T> T on(ThriftServicesDiscovery thriftServicesDb, Class<T> cls) {
        return onIfaceAsAsync(thriftServicesDb, cls);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public static <T> T onIfaceAsAsync(ThriftServicesDiscovery thriftServicesDb, Class<T> cls) {

        return (T) Proxy.newProxyInstance(ThriftProxyFactory.class.getClassLoader(), new Class[]{cls},
                                          new ServiceIfaceProxy(thriftServicesDb, ii -> {
                                              ThriftCallFutureHolder.thriftCallFuture.set(ii);
                                              throw new NullResult();
                                          }));
    }

}
