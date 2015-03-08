package com.knockchat.appserver.transport;

public interface TransportEventsIF {
	
    void onConnect();
    void onClose();
    void onConnectError();

}
