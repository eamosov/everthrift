package org.everthrift.jetty.transport.http;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.everthrift.appserver.controller.ThriftProcessor;

public class BinaryThriftServlet extends AbstractThriftServlet {

    private static final long serialVersionUID = 1L;

    private final static TProtocolFactory factory = new TBinaryProtocol.Factory();

    public BinaryThriftServlet(ThriftProcessor thriftProcessor) {
        super(thriftProcessor);
    }

    @Override
    protected String getContentType() {
        return "application/x-thrift";
    }

    @Override
    protected TProtocolFactory getProtocolFactory() {
        return factory;
    }

}
