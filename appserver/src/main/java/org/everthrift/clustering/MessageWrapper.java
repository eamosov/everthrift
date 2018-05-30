package org.everthrift.clustering;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.apache.thrift.transport.TMemoryBuffer;
import org.apache.thrift.transport.TMemoryInputTransport;
import org.apache.thrift.transport.TTransport;
import org.jetbrains.annotations.NotNull;
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

    public static String HTTP_REQUEST_PARAMS = "HTTP_REQUEST_PARAMS";

    public static String HTTP_COOKIES = "HTTP_COOKIES";

    public static String HTTP_HEADERS = "HTTP_HEADERS";

    public static String HTTP_X_REAL_IP = "X-Real-IP";

    private TTransport tTransport;

    private Map<String, Object> attributes;

    public MessageWrapper(TTransport tTransport) {
        super();
        this.tTransport = tTransport;
        this.attributes = new HashMap<>();
    }

    @Override
    public synchronized String toString() {
        return "MessageWrapper [tTransport=" + tTransport + ", attributes=" + attributes + "]";
    }

    public synchronized MessageWrapper putAllAttributes(@NotNull Map<String, Object> attributes) {
        this.attributes.putAll(attributes);
        return this;
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

    public synchronized Object getAttribute(@NotNull String name) {
        return attributes.get(name);
    }

    @NotNull
    public synchronized Map<String, Object> getAttributes() {
        return ImmutableMap.copyOf(attributes);
    }

    public synchronized Object removeAttribute(@NotNull String name) {
        return attributes.remove(name);
    }
}
