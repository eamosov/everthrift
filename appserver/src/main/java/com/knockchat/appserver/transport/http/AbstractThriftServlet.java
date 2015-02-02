package com.knockchat.appserver.transport.http;

import java.io.IOException;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.AutoExpandingBufferWriteTransport;
import org.apache.thrift.transport.TMemoryInputTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.terracotta.license.util.IOUtils;

import com.knockchat.appserver.controller.ThriftProcessor;
import com.knockchat.appserver.controller.ThriftProcessorFactory;

public abstract class AbstractThriftServlet extends HttpServlet implements InitializingBean {
	
	private static final long serialVersionUID = 1L;

	private static final Logger log = LoggerFactory.getLogger(AbstractThriftServlet.class);
	
	@Autowired
	private ThriftProcessorFactory tpf;
	
	@Autowired
	private RpcHttpRegistry registry;

	private ThriftProcessor tp;
	
	protected abstract String getContentType();
	
	protected abstract TProtocolFactory getProtocolFactory();
	
	@Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws IOException {
				
		log.debug("Handle /thrift request");
		
		final byte[] packet = IOUtils.readBytes(request.getInputStream());
		
		final TMemoryInputTransport it = new TMemoryInputTransport(packet);
		final AutoExpandingBufferWriteTransport ot = new AutoExpandingBufferWriteTransport(1024, 1.5);
		
		final TProtocol in = getProtocolFactory().getProtocol(it);
		final TProtocol out = getProtocolFactory().getProtocol(ot);

		try {
			tp.process(in, out);
			
			response.setContentType(getContentType());
			response.setContentLength(ot.getPos());
			response.getOutputStream().write(ot.getBuf().array());
			response.flushBuffer();			
		} catch (TException e) {
			
			log.error("TException", e);
			
			response.setStatus(500);
			response.setContentType("text/plain");
			response.getOutputStream().write(ExceptionUtils.getMessage(e).getBytes());
			response.flushBuffer();
		}
    }

	@Override
	public void afterPropertiesSet() throws Exception {
        tp = tpf.getThriftProcessor(registry, getProtocolFactory());	
	}


}