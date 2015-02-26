package com.knockchat.appserver.transport.http;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.springframework.stereotype.Component;

@Component
public class BinaryThriftServlet extends AbstractThriftServlet{
	
	private static final long serialVersionUID = 1L;
	
	private final static TProtocolFactory factory = new TBinaryProtocol.Factory();

	@Override
	protected String getContentType(){
		return  "application/x-thrift";
	}
	
	protected TProtocolFactory getProtocolFactory(){
		return factory;
	}

}
