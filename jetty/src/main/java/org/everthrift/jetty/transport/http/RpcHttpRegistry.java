package org.everthrift.jetty.transport.http;

import org.everthrift.appserver.controller.ThriftControllerRegistry;
import org.everthrift.appserver.transport.http.RpcHttp;

public class RpcHttpRegistry extends ThriftControllerRegistry {

    public RpcHttpRegistry() {
        super(RpcHttp.class);
    }
}
