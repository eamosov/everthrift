package org.everthrift.clustering.jgroups;

import org.apache.thrift.TException;
import org.everthrift.thrift.ThriftCallFuture;
import org.everthrift.clustering.thrift.ThriftCallFutureHolder;
import org.everthrift.clustering.thrift.ThriftProxyFactory;
import org.everthrift.thrift.ThriftServicesDiscovery;
import org.jgroups.Address;
import org.jgroups.blocks.ResponseMode;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public interface ClusterThriftClientIF {

    static <T> T on(Class<T> cls) {
        return ThriftProxyFactory.on(ThriftServicesDiscovery.INSTANCE, cls);
    }

    interface Reply<T> {
        T get() throws TException;

        default T getUnchecked() {
            try {
                return get();
            } catch (TException e) {
                throw new RuntimeException(e);
            }
        }
    }

    class Options {
        private Options() {
        }

        public static Timeout timeout(int timeout) {
            return new Timeout(timeout);
        }

        public static Loopback loopback(boolean loopback) {
            return new Loopback(loopback);
        }

        public static Response responseMode(ResponseMode responseMode) {
            return new Response(responseMode);
        }
    }

    class Timeout extends Options {
        public final int timeout;

        private Timeout(int timeout) {
            this.timeout = timeout;
        }
    }

    class Loopback extends Options {
        public final boolean loopback;

        private Loopback(boolean loopback) {
            this.loopback = loopback;
        }
    }

    class Response extends Options {
        public final ResponseMode responseMode;

        private Response(ResponseMode responseMode) {
            this.responseMode = responseMode;
        }
    }

    default <T> CompletableFuture<Map<Address, Reply<T>>> call(T methodCall, Map<String, Object> attributes, Options... options) throws TException {
        return call(ThriftCallFutureHolder.getThriftCallFuture(), attributes, options);
    }

    @SuppressWarnings("rawtypes")
    default <T> CompletableFuture<Map<Address, Reply<T>>> call(final ThriftCallFuture<T> ii, Map<String, Object> attributes, Options... options) throws TException {
        return call(null, null, ii, attributes, options);
    }

    default <T> CompletableFuture<T> call(Address destination, T methodCall, Map<String, Object> attributes, Options... options) throws TException {
        return call(destination, ThriftCallFutureHolder.getThriftCallFuture(), attributes, options);
    }

    @SuppressWarnings("rawtypes")
    default <T> CompletableFuture<T> call(Address destination, ThriftCallFuture<T> ii, Map<String, Object> attributes, Options... options) throws TException {
        final CompletableFuture<Map<Address, Reply<T>>> ret = call(Collections.singleton(destination), null, ii, attributes, options);

        final CompletableFuture<T> s = new CompletableFuture<T>();

        ret.whenComplete((m, t) -> {
            if (t != null) {
                s.completeExceptionally(t);
            } else {
                try {
                    s.complete(m.get(destination).get());
                } catch (TException e) {
                    s.completeExceptionally(e);
                }
            }
        });

        return s;
    }

    <T> CompletableFuture<Map<Address, Reply<T>>> call(Collection<Address> dest, Collection<Address> exclusionList,
                                                       ThriftCallFuture<T> tInfo, Map<String, Object> attributes, Options... options) throws TException;

    default <T> CompletableFuture<T> callOne(T methodCall, Map<String, Object> attributes, Options... options) throws TException {
        return callOne(ThriftCallFutureHolder.getThriftCallFuture(), attributes, options);
    }

    <T> CompletableFuture<T> callOne(ThriftCallFuture<T> ii, Map<String, Object> attributes, Options... options) throws TException;

    <T> CompletableFuture<T> callOne(List<Address> destination, ThriftCallFuture<T> ii, Map<String, Object> attributes, Options... options) throws TException;
}
