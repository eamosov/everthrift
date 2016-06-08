package com.knockchat.jetty.transport.http;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.io.IOUtils;
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
import org.springframework.context.ApplicationContext;

import com.knockchat.appserver.controller.ThriftProcessor;
import com.knockchat.clustering.MessageWrapper;

public abstract class AbstractThriftServlet extends HttpServlet implements InitializingBean {
	
	private static final long serialVersionUID = 1L;

	private static final Logger log = LoggerFactory.getLogger(AbstractThriftServlet.class);
	
	@Autowired
	private ApplicationContext context;
	
	@Autowired
	private RpcHttpRegistry registry;

	private ThriftProcessor tp;
	
	protected abstract String getContentType();
	
	protected abstract TProtocolFactory getProtocolFactory();
	
    protected void doOptions(HttpServletRequest req, HttpServletResponse response) throws ServletException, IOException {
		response.setHeader("Access-Control-Allow-Origin", "*");
		response.setHeader("Access-Control-Allow-Headers", "Content-Type, Access-Control-Allow-Headers, Authorization, X-Requested-With");
		super.doOptions(req, response);
    }
	
	@Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws IOException {
				
		log.debug("Handle thrift request on THttpTransport");
		
		response.setHeader("Access-Control-Allow-Origin", "*");
		response.setHeader("Access-Control-Allow-Headers", "Content-Type, Access-Control-Allow-Headers, Authorization, X-Requested-With");
		
		final byte[] packet = IOUtils.toByteArray(request.getInputStream());
		
		final TMemoryInputTransport it = new TMemoryInputTransport(packet);
		final AutoExpandingBufferWriteTransport ot = new AutoExpandingBufferWriteTransport(1024, 1.5);
		
		final TProtocol in = getProtocolFactory().getProtocol(it);
		final TProtocol out = getProtocolFactory().getProtocol(ot);
		
		try {
			final MessageWrapper mw = new MessageWrapper(null).setHttpRequestParams(request.getParameterMap());
			final String xRealIp = request.getHeader(MessageWrapper.HTTP_X_REAL_IP);
			if (xRealIp != null)
				mw.setAttribute(MessageWrapper.HTTP_X_REAL_IP, xRealIp);
			else
				mw.setAttribute(MessageWrapper.HTTP_X_REAL_IP, request.getRemoteHost() + ":" + request.getRemotePort());
			
			tp.process(in, out, mw);
			
			response.setContentType(getContentType());
			response.setContentLength(ot.getPos());
			response.getOutputStream().write(ot.getBuf().array(), 0, ot.getPos());
			response.flushBuffer();			
		} catch (TException e) {
			
			log.error("TException", e);
			
			response.setStatus(500);
			response.setContentType("text/plain");
			response.getOutputStream().write(ExceptionUtils.getMessage(e).getBytes());
			response.flushBuffer();
		} catch (RuntimeException e){
			
			response.setStatus(500);
			response.setContentType("text/plain");
			response.getOutputStream().write("Server error, see log files".getBytes());
			response.flushBuffer();			
		}
    }

	@Override
	public void afterPropertiesSet() throws Exception {
        tp = ThriftProcessor.create(context, registry, getProtocolFactory());	
	}


}
