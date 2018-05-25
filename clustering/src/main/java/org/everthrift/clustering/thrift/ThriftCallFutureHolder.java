package org.everthrift.clustering.thrift;

public class ThriftCallFutureHolder {

    final static ThreadLocal<ThriftCallFuture<?>> thriftCallFuture = new ThreadLocal<ThriftCallFuture<?>>();

    public static ThriftCallFuture<?> getThriftCallFuture() {
        return thriftCallFuture.get();
    }

}
