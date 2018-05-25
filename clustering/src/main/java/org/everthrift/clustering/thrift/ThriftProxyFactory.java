package org.everthrift.clustering.thrift;

import org.everthrift.utils.ThriftServicesDb;

import java.lang.reflect.Proxy;

public class ThriftProxyFactory {

    /**
     * alias for onIfaceAsAsync
     *
     * @param cls
     * @return
     */
    public static <T> T on(ThriftServicesDb thriftServicesDb, Class<T> cls) {
        return onIfaceAsAsync(thriftServicesDb, cls);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public static <T> T onIfaceAsAsync(ThriftServicesDb thriftServicesDb, Class<T> cls) {

        return (T) Proxy.newProxyInstance(ThriftProxyFactory.class.getClassLoader(), new Class[]{cls},
                                          new ServiceIfaceProxy(thriftServicesDb, ii -> {
                                              ThriftCallFutureHolder.thriftCallFuture.set(ii);
                                              throw new NullResult();
                                          }));
    }

}
