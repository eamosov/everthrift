package org.everthrift.jetty.transport.websocket;

import java.util.Collections;
import java.util.Map;
import java.util.UUID;

import javax.servlet.http.Cookie;

import org.everthrift.clustering.MessageWrapper;
import org.springframework.http.server.ServerHttpRequest;
import org.springframework.http.server.ServerHttpResponse;
import org.springframework.http.server.ServletServerHttpRequest;
import org.springframework.web.socket.WebSocketHandler;
import org.springframework.web.socket.server.HandshakeInterceptor;

import com.google.common.base.Optional;

public class WebsocketHandshakeInterceptor implements HandshakeInterceptor {

    public WebsocketHandshakeInterceptor() {

    }

    @Override
    public boolean beforeHandshake(ServerHttpRequest request, ServerHttpResponse response, WebSocketHandler wsHandler, Map<String, Object> attributes) throws Exception {

        attributes.put(WebsocketThriftHandler.UUID, UUID.randomUUID().toString());
        attributes.put(MessageWrapper.HTTP_REQUEST_PARAMS, Optional.fromNullable(((ServletServerHttpRequest)request).getServletRequest().getParameterMap()).or(Collections.emptyMap()));
        attributes.put(MessageWrapper.HTTP_COOKIES,  Optional.fromNullable(((ServletServerHttpRequest)request).getServletRequest().getCookies()).or( () -> (new Cookie[0])));

        final String xRealIp = request.getHeaders().getFirst(WebsocketThriftHandler.HTTP_X_REAL_IP);
        if (xRealIp !=null)
            attributes.put(WebsocketThriftHandler.HTTP_X_REAL_IP, xRealIp);

        return true;
    }

    @Override
    public void afterHandshake(ServerHttpRequest request, ServerHttpResponse response, WebSocketHandler wsHandler, Exception exception) {

    }

}
