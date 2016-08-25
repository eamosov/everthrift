package org.everthrift.jetty.transport.websocket;

import org.everthrift.appserver.controller.ThriftControllerRegistry;
import org.everthrift.appserver.transport.websocket.RpcWebsocket;

public class RpcWebsocketRegistry extends ThriftControllerRegistry {

    public RpcWebsocketRegistry() {
        super(RpcWebsocket.class);
    }

}
