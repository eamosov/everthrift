package com.knockchat.cli;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import com.knockchat.appserver.thrift.cluster.ClusterConfiguration;
import com.knockchat.appserver.thrift.cluster.ClusterException;
import com.knockchat.appserver.thrift.cluster.ClusterService;

public class ThriftUtills {

    public static final String DEFAULT_VERSION = "0.0.1";

    public static TProtocol getProtocol(String host, int port)
            throws TTransportException {
        TTransport t = new TFramedTransport(new TSocket(host, port));
        t.open();
        TProtocol p = new TBinaryProtocol(t);
        return p;
    }

    public static ClusterConfiguration getClusterConfiguration(String host, int port) throws ClusterException, TTransportException, TException {
       return new ClusterService.Client(getProtocol(host, port)).getClusterConfiguration();
    }

}
