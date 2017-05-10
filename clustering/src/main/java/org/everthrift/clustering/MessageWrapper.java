package org.everthrift.clustering;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.apache.thrift.transport.TMemoryBuffer;
import org.apache.thrift.transport.TMemoryInputTransport;
import org.apache.thrift.transport.TTransport;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHeaders;

import java.io.IOException;
import java.io.NotSerializableException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

public class MessageWrapper implements Serializable {

    private static final long serialVersionUID = 1L;

    /* attributes */
    public static String SESSION_ID = "SESSION_ID";

    public static String WS_CONTENT_TYPE = "WS_CONTENT_TYPE";

    public static String OUT_CHANNEL = "OUT_CHANNEL";

    public static String MESSAGE_HEADERS = "MESSAGE_HEADERS";

    public static String HTTP_REQUEST_PARAMS = "HTTP_REQUEST_PARAMS";

    public static String HTTP_COOKIES = "HTTP_COOKIES";

    public static String HTTP_HEADERS = "HTTP_HEADERS";

    public static String HTTP_X_REAL_IP = "X-Real-IP";

    private TTransport tTransport;

    private Map<String, Object> attributes;

    public static enum WebsocketContentType {
        BINARY,
        TEXT
    }

    public MessageWrapper(TTransport tTransport) {
        super();
        this.tTransport = tTransport;
        this.attributes = new HashMap<>();
    }

    @Override
    public synchronized String toString() {
        return "MessageWrapper [tTransport=" + tTransport + ", attributes=" + attributes + "]";
    }

    public synchronized MessageWrapper copySerializeableAttributes(MessageWrapper old) {
        for (Entry<String, Object> e : old.attributes.entrySet()) {
            if (e.getValue() instanceof Serializable) {
                attributes.put(e.getKey(), e.getValue());
            }
        }
        return this;
    }

    public synchronized MessageWrapper copyAttributes(MessageWrapper old) {
        copyAttributes(old.attributes);
        return this;
    }

    public synchronized MessageWrapper copyAttributes(Map<String, Object> attributes) {
        this.attributes.putAll(attributes);
        return this;
    }

    public synchronized MessageWrapper setSessionId(String sessionId) {
        attributes.put(SESSION_ID, sessionId);
        return this;
    }

    public synchronized String getSessionId() {
        return (String) attributes.get(SESSION_ID);
    }

    public synchronized MessageWrapper setWebsocketContentType(WebsocketContentType websocketContentType) {
        attributes.put(WS_CONTENT_TYPE, websocketContentType);
        return this;
    }

    public synchronized WebsocketContentType getWebsocketContentType() {
        return (WebsocketContentType) attributes.get(WS_CONTENT_TYPE);
    }

    public synchronized MessageWrapper setOutChannel(MessageChannel outChannel) {
        attributes.put(OUT_CHANNEL, outChannel);
        return this;
    }

    public synchronized MessageChannel getOutChannel() {
        return (MessageChannel) attributes.get(OUT_CHANNEL);
    }

    public synchronized MessageWrapper setMessageHeaders(MessageHeaders messageHeaders) {
        attributes.put(MESSAGE_HEADERS, messageHeaders);
        return this;
    }

    public synchronized MessageHeaders getMessageHeaders() {
        return (MessageHeaders) attributes.remove(MESSAGE_HEADERS);
    }

    public synchronized MessageWrapper removeCorrelationHeaders() {
        attributes.remove(MESSAGE_HEADERS);
        attributes.remove(OUT_CHANNEL);
        return this;
    }

    public MessageWrapper toSerializable() {
        return new MessageWrapper(tTransport).copySerializeableAttributes(this);
    }

    public TTransport getTTransport() {
        return tTransport;
    }

    public synchronized Map<String, String[]> getHttpRequestParams() {
        return (Map) attributes.get(HTTP_REQUEST_PARAMS);
    }

    public synchronized MessageWrapper setHttpRequestParams(Map<String, String[]> params) {
        attributes.put(HTTP_REQUEST_PARAMS, params);
        return this;
    }

    public synchronized Map<String, String> getHttpHeaders() {
        return (Map<String, String>) attributes.get(MessageWrapper.HTTP_HEADERS);
    }

    private synchronized void writeObject(ObjectOutputStream oos) throws IOException {

        if (tTransport == null) {
            oos.writeObject(null);
        } else if (tTransport instanceof TMemoryBuffer) {
            oos.writeObject(((TMemoryBuffer) tTransport).toByteArray());
        } else {
            throw new NotSerializableException(tTransport.getClass().getCanonicalName());
        }

        if (!attributes.isEmpty()) {
            oos.writeObject(attributes);
        } else {
            oos.writeObject(null);
        }
    }

    private synchronized void readObject(ObjectInputStream ois) throws ClassNotFoundException, IOException {
        byte[] tt = (byte[]) ois.readObject();
        if (tt == null) {
            tTransport = null;
        } else {
            tTransport = new TMemoryInputTransport(tt);
        }

        attributes = (Map) ois.readObject();
        if (attributes == null) {
            attributes = Maps.newHashMap();
        }
    }

    public synchronized Object getAttribute(String name) {
        return attributes.get(name);
    }

    public synchronized Map<String, Object> getAttributes() {
        return ImmutableMap.copyOf(attributes);
    }

    public synchronized Object removeAttribute(String name) {
        return attributes.remove(name);
    }

    public synchronized MessageWrapper setAttribute(String name, Object value) {
        attributes.put(name, value);
        return this;
    }
}
