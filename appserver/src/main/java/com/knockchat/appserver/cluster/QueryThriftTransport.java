package com.knockchat.appserver.cluster;

public interface QueryThriftTransport {
	
	public <T> T onIface(Class<T> cls);
	
}
