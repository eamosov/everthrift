package org.everthrift.clustering.thrift;

import org.everthrift.thrift.ThriftCallFuture;

public class ThriftCallFutureHolder {

    final static ThreadLocal<ThriftCallFuture<?>> thriftCallFuture = new ThreadLocal<ThriftCallFuture<?>>();

    public static ThriftCallFuture<?> getThriftCallFuture() {
        return thriftCallFuture.get();
    }

}
