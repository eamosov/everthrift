package org.everthrift.jetty.transport.http;

import org.apache.thrift.protocol.TJSONProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.everthrift.appserver.controller.ThriftProcessor;

public class JsonThriftServlet extends AbstractThriftServlet {

    private static final long serialVersionUID = 1L;

    private final static TProtocolFactory factory = new TJSONProtocol.Factory();

    public JsonThriftServlet(ThriftProcessor thriftProcessor) {
        super(thriftProcessor);
    }

    @Override
    protected String getContentType() {
        return "application/json";
    }

    @Override
    protected TProtocolFactory getProtocolFactory() {
        return factory;
    }

}
