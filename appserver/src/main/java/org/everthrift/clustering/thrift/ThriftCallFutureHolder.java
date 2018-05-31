package org.everthrift.clustering.thrift;

import org.everthrift.thrift.ThriftCallFuture;

public class ThriftCallFutureHolder {

    final static ThreadLocal<ThriftCallFuture<?>> thriftCallFuture = new ThreadLocal<ThriftCallFuture<?>>();

    public static <T> ThriftCallFuture<T> getThriftCallFuture() {
        return (ThriftCallFuture)thriftCallFuture.get();
    }

}
