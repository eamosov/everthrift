package org.everthrift.thriftclient.transport;

import org.apache.thrift.transport.TTransport;

public abstract class TAsyncTransport extends TTransport {

	public abstract void setEventsHandler(TransportEventsIF eventsHandler);
}
