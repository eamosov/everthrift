package org.everthrift.jms;

import org.everthrift.appserver.controller.ThriftControllerRegistry;
import org.everthrift.appserver.transport.jms.RpcJms;

import java.util.List;

public class RpcJmsRegistry extends ThriftControllerRegistry {

    public RpcJmsRegistry() {
        super(RpcJms.class);
    }

}
