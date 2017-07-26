package org.everthrift.appserver.transport.tcp;

import org.everthrift.appserver.controller.ThriftControllerRegistry;

import java.util.List;

public class RpcSyncTcpRegistry extends ThriftControllerRegistry {

    public RpcSyncTcpRegistry(final List<String> basePaths) {
        super(RpcSyncTcp.class, basePaths);
    }

}
