package com.knockchat.appserver.transport.http;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocolFactory;

public class BinaryThriftServlet extends AbstractThriftServlet{
	
	private static final long serialVersionUID = 1L;
	
	private final static TProtocolFactory factory = new TBinaryProtocol.Factory();

	@Override
	protected String getContentType(){
		return  "application/x-thrift";
	}
	
	@Override
	protected TProtocolFactory getProtocolFactory(){
		return factory;
	}

}
