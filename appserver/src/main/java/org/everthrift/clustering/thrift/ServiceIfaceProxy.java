package org.everthrift.clustering.thrift;

import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.everthrift.thrift.ThriftCallFuture;
import org.everthrift.thrift.ThriftServicesDiscovery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;

@SuppressWarnings({"rawtypes", "unchecked"})
public class ServiceIfaceProxy implements InvocationHandler {
    static final Logger log = LoggerFactory.getLogger(ServiceIfaceProxy.class);

    public interface ServiceIfaceProxyCallback {
        Object call(ThriftCallFuture ii) throws NullResult, TException;
    }

    final ThriftServicesDiscovery thriftServicesDb;

    final ServiceIfaceProxyCallback callback;

    /**
     * @param callback после вызова любого метода сервиса будет вызван
     *                 callback.set
     */
    public ServiceIfaceProxy(ThriftServicesDiscovery thriftServicesDiscovery, ServiceIfaceProxyCallback callback) {
        this.callback = callback;
        this.thriftServicesDb = thriftServicesDiscovery;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {

        try {
            final ThriftServicesDiscovery.ThriftMethodEntry e = thriftServicesDb.getByMethod(method);
            if (e == null) {
                throw new RuntimeException("Thrift service not found: " + method.toString());
            }

            final TBase _args = e.argsConstructor.newInstance(args);
            return callback.call(new ThriftCallFuture(e, _args));
        } catch (NullResult e) {
            final Class rt = method.getReturnType();
            if (rt == Boolean.TYPE) {
                return false;
            } else if (rt == Character.TYPE) {
                return ' ';
            } else if (rt == Byte.TYPE) {
                return (byte) 0;
            } else if (rt == Short.TYPE) {
                return (short) 0;
            } else if (rt == Integer.TYPE) {
                return 0;
            } else if (rt == Long.TYPE) {
                return (long) 0;
            } else if (rt == Float.TYPE) {
                return 0f;
            } else if (rt == Double.TYPE) {
                return (double) 0;
            } else {
                return null;
            }
        }
    }

}
