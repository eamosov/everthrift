package com.knockchat.appserver.transport.http;

import org.apache.thrift.protocol.TJSONProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.springframework.stereotype.Component;

@Component
public class JsonThriftServlet extends AbstractThriftServlet{
	
	private static final long serialVersionUID = 1L;
	
	private final static TProtocolFactory factory = new TJSONProtocol.Factory();

	@Override
	protected String getContentType(){
		return  "application/json";
	}
	
	@Override
	protected TProtocolFactory getProtocolFactory(){
		return factory;
	}

}
