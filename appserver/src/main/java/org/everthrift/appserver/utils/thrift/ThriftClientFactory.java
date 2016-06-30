package org.everthrift.appserver.utils.thrift;

public interface ThriftClientFactory {
    ThriftClient getThriftClient(String sessionId);
}
