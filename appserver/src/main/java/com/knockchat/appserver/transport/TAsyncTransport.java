package com.knockchat.appserver.transport;

import org.apache.thrift.transport.TTransport;

public abstract class TAsyncTransport extends TTransport {

	public abstract void setEventsHandler(TransportEventsIF eventsHandler);
}
