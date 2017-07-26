package org.everthrift.jetty.transport.websocket;

import org.everthrift.appserver.controller.ThriftControllerRegistry;
import org.everthrift.appserver.transport.websocket.RpcWebsocket;

import java.util.List;

public class RpcWebsocketRegistry extends ThriftControllerRegistry {

    public RpcWebsocketRegistry(List<String> basePath) {
        super(RpcWebsocket.class, basePath);
    }

}
