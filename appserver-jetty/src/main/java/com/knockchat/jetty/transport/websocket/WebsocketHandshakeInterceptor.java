package com.knockchat.jetty.transport.websocket;

import java.util.Map;
import java.util.UUID;

import org.springframework.http.server.ServerHttpRequest;
import org.springframework.http.server.ServerHttpResponse;
import org.springframework.http.server.ServletServerHttpRequest;
import org.springframework.web.socket.WebSocketHandler;
import org.springframework.web.socket.server.HandshakeInterceptor;

import com.knockchat.clustering.MessageWrapper;

public class WebsocketHandshakeInterceptor implements HandshakeInterceptor {

	public WebsocketHandshakeInterceptor() {
		
	}

	@Override
	public boolean beforeHandshake(ServerHttpRequest request, ServerHttpResponse response, WebSocketHandler wsHandler, Map<String, Object> attributes) throws Exception {
		
		attributes.put(WebsocketThriftHandler.UUID, UUID.randomUUID().toString());
		attributes.put(MessageWrapper.HTTP_REQUEST_PARAMS, ((ServletServerHttpRequest)request).getServletRequest().getParameterMap());
		
		final String xRealIp = request.getHeaders().getFirst(WebsocketThriftHandler.HTTP_X_REAL_IP);
		if (xRealIp !=null)
			attributes.put(WebsocketThriftHandler.HTTP_X_REAL_IP, xRealIp);
			
		return true;
	}

	@Override
	public void afterHandshake(ServerHttpRequest request, ServerHttpResponse response, WebSocketHandler wsHandler, Exception exception) {

	}

}
