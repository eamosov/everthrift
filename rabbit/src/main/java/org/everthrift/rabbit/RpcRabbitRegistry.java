package org.everthrift.rabbit;

import org.everthrift.appserver.controller.ThriftControllerRegistry;

import java.util.List;

public class RpcRabbitRegistry extends ThriftControllerRegistry {

    public RpcRabbitRegistry() {
        super(RpcRabbit.class);
    }

}
