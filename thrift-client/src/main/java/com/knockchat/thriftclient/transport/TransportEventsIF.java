package com.knockchat.thriftclient.transport;

public interface TransportEventsIF {
	
    void onConnect();
    void onClose();
    void onConnectError();

}
