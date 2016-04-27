package com.knockchat.appserver.transport.websocket;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.WebSocketHandler;
import org.springframework.web.socket.WebSocketMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.LoggingWebSocketHandlerDecorator;

public class ThriftHandler implements WebSocketHandler {
	
	private static final Logger log = LoggerFactory.getLogger(ThriftHandler.class);

	private WebSocketHandler delegate;
	
	public ThriftHandler() {

	}
	
	@Override
	public void afterConnectionEstablished(WebSocketSession session) throws Exception {
		this.delegate.afterConnectionEstablished(session);
	}

	@Override
	public void handleMessage(WebSocketSession session, WebSocketMessage<?> message) throws Exception {
		this.delegate.handleMessage(session, message);
	}

	@Override
	public void handleTransportError(WebSocketSession session, Throwable exception) throws Exception {
		this.delegate.handleTransportError(session, exception);
	}

	@Override
	public void afterConnectionClosed(WebSocketSession session, CloseStatus closeStatus) throws Exception {
		this.delegate.afterConnectionClosed(session, closeStatus);
	}

	@Override
	public boolean supportsPartialMessages() {
		return this.delegate.supportsPartialMessages();
	}

	@Override
	public String toString() {
		return super.toString();
	}

	public void setHandler(WebSocketHandler handler) {
		this.delegate = new LoggingWebSocketHandlerDecorator(handler);
	}
	
}
