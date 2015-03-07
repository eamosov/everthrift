package com.knockchat.appserver.transport.websocket;

import java.util.Map;
import java.util.UUID;

import org.springframework.http.server.ServerHttpRequest;
import org.springframework.http.server.ServerHttpResponse;
import org.springframework.http.server.ServletServerHttpRequest;
import org.springframework.web.socket.WebSocketHandler;
import org.springframework.web.socket.server.HandshakeInterceptor;

import com.knockchat.appserver.controller.MessageWrapper;

public class WebsocketHandshakeInterceptor implements HandshakeInterceptor {

	public WebsocketHandshakeInterceptor() {
		
	}

	@Override
	public boolean beforeHandshake(ServerHttpRequest request, ServerHttpResponse response, WebSocketHandler wsHandler, Map<String, Object> attributes) throws Exception {
		
		attributes.put(WebsocketThriftHandler.UUID, UUID.randomUUID().toString());
		attributes.put(MessageWrapper.HTTP_REQUEST_PARAMS, ((ServletServerHttpRequest)request).getServletRequest().getParameterMap());
		return true;
	}

	@Override
	public void afterHandshake(ServerHttpRequest request, ServerHttpResponse response, WebSocketHandler wsHandler, Exception exception) {

	}

}
