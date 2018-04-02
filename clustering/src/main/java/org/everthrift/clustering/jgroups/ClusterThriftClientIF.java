package org.everthrift.clustering.jgroups;

import org.apache.thrift.TException;
import org.everthrift.clustering.thrift.InvocationInfo;
import org.everthrift.clustering.thrift.InvocationInfoThreadHolder;
import org.everthrift.clustering.thrift.ThriftProxyFactory;
import org.jgroups.Address;
import org.jgroups.blocks.ResponseMode;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public interface ClusterThriftClientIF {

    static <T> T on(Class<T> cls) {
        return ThriftProxyFactory.on(cls);
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
        return call(InvocationInfoThreadHolder.getInvocationInfo(), attributes, options);
    }

    @SuppressWarnings("rawtypes")
    default <T> CompletableFuture<Map<Address, Reply<T>>> call(final InvocationInfo ii, Map<String, Object> attributes, Options... options) throws TException {
        return call(null, null, ii, attributes, options);
    }

    default <T> CompletableFuture<T> call(Address destination, T methodCall, Map<String, Object> attributes, Options... options) throws TException {
        return call(destination, InvocationInfoThreadHolder.getInvocationInfo(), attributes, options);
    }

    @SuppressWarnings("rawtypes")
    default public <T> CompletableFuture<T> call(Address destination, InvocationInfo ii, Map<String, Object> attributes, Options... options) throws TException {
        final CompletableFuture<Map<Address, Reply<T>>> ret = call(Collections.singleton(destination), null, ii, attributes, options);

        final CompletableFuture<T> s = new CompletableFuture();

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

    @SuppressWarnings("rawtypes")
    <T> CompletableFuture<Map<Address, Reply<T>>> call(Collection<Address> dest, Collection<Address> exclusionList,
                                                       InvocationInfo tInfo, Map<String, Object> attributes, Options... options) throws TException;

    default <T> CompletableFuture<T> callOne(T methodCall, Map<String, Object> attributes, Options... options) throws TException {
        return callOne(InvocationInfoThreadHolder.getInvocationInfo(), attributes, options);
    }

    @SuppressWarnings("rawtypes")
    <T> CompletableFuture<T> callOne(InvocationInfo ii, Map<String, Object> attributes, Options... options) throws TException;
}
