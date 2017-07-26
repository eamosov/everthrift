package org.everthrift.jetty.transport.http;

import org.everthrift.appserver.controller.ThriftControllerRegistry;
import org.everthrift.appserver.transport.http.RpcHttp;

import java.util.List;

public class RpcHttpRegistry extends ThriftControllerRegistry {

    public RpcHttpRegistry(List<String> basePath) {
        super(RpcHttp.class, basePath);
    }
}
