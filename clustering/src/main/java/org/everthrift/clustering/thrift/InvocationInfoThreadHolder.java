package org.everthrift.clustering.thrift;

public class InvocationInfoThreadHolder {

    final static ThreadLocal<InvocationInfo<?>> invocationInfo = new ThreadLocal<InvocationInfo<?>>();

    public static InvocationInfo<?> getInvocationInfo() {
        return invocationInfo.get();
    }

}
