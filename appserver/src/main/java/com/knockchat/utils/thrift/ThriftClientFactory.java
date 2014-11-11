package com.knockchat.utils.thrift;

public interface ThriftClientFactory {
	ThriftClient getThriftClient(String sessionId);
}
