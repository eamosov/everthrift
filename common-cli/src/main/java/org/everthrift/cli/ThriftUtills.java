package org.everthrift.cli;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

public class ThriftUtills {

    public static final String DEFAULT_VERSION = "0.0.1";

    public static TProtocol getProtocol(String host, int port) throws TTransportException {
        TTransport t = new TFramedTransport(new TSocket(host, port));
        t.open();
        TProtocol p = new TBinaryProtocol(t);
        return p;
    }

}
