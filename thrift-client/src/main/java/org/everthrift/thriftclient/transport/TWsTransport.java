package org.everthrift.thriftclient.transport;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.NotYetConnectedException;
import java.nio.charset.Charset;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;

import org.apache.thrift.TApplicationException;
import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TMessage;
import org.apache.thrift.protocol.TMessageType;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.protocol.TProtocolUtil;
import org.apache.thrift.protocol.TType;
import org.apache.thrift.transport.TMemoryBuffer;
import org.apache.thrift.transport.TMemoryInputTransport;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.transport.TTransportFactory;
import org.everthrift.clustering.thrift.InvocationInfo;
import org.everthrift.thrift.AsyncRegister;
import org.java_websocket.client.WebSocketClient;
import org.java_websocket.drafts.Draft_17;
import org.java_websocket.handshake.ServerHandshake;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;
import com.google.common.util.concurrent.SettableFuture;

public class TWsTransport extends TAsyncTransport {

    private static final Charset UTF_8 = Charset.forName("UTF-8");

    private static final Logger log = LoggerFactory.getLogger(TWsTransport.class);

    private URI uri;

    private long connectTimeout;

    private static final ByteBuffer EOF = ByteBuffer.wrap(new byte[1]);

    private final ThreadLocal<ByteArrayOutputStream> requestBuffer_ = new ThreadLocal<ByteArrayOutputStream>() {
        @Override
        protected ByteArrayOutputStream initialValue() {
            return new ByteArrayOutputStream();
        }
    };

    private Websocket websocket;

    private final LinkedBlockingDeque<ByteBuffer> queue = new LinkedBlockingDeque<ByteBuffer>();

    private byte[] pendingBytes;

    private int pendingBytesPos;

    private final TProtocolFactory protocolFactory;

    private final TTransportFactory transportFactory;

    private final TProcessor processor;

    private SettableFuture<Websocket> connectFuture;

    private boolean isConnected = false;

    private boolean isOpened = false;

    private boolean closed = false;

    private final AsyncRegister async;

    private TransportEventsIF eventsHandler;

    private final ExecutorService executor;

    /**
     * Нужно сохранять идентификаторы отправленных пакетов, чтобы вызвать
     * исключения в случае потери соединения
     */
    private Set<Integer> pendingRequests = Sets.newHashSet();

    /**
     *
     * @param uri
     * @param connectTimeout - milliseconds
     * @param processor
     * @param protocolFactory
     * @param transportFactory
     * @param async
     * @param executor
     */
    public TWsTransport(URI uri, long timeout, TProcessor processor, TProtocolFactory protocolFactory, TTransportFactory transportFactory,
                        AsyncRegister async, ExecutorService executor) {

        // WebSocketImpl.DEBUG = true;

        this.uri = uri;
        this.connectTimeout = timeout;
        this.processor = processor;
        this.protocolFactory = protocolFactory;
        this.transportFactory = transportFactory;
        this.async = async;
        this.executor = executor;
    }

    @Override
    public synchronized boolean isOpen() {
        return isConnected;
    }

    @Override
    public void open() throws TTransportException {
        openAsync();
        waitForConnect();
    }

    public synchronized void openAsync() throws TTransportException {

        log.debug("open websocket, URI:{}", uri.toString());

        if (closed)
            throw new TTransportException(TTransportException.NOT_OPEN, "closed");

        if (isConnected || isOpened)
            throw new TTransportException(TTransportException.ALREADY_OPEN, "ALREADY_OPEN");

        isOpened = true;

        connectFuture = SettableFuture.create();

        websocket = new Websocket(uri);

        if (uri.getScheme().equalsIgnoreCase("wss")) {

            final SSLContext sslContext;
            try {
                sslContext = SSLContext.getInstance("TLS");
            }
            catch (NoSuchAlgorithmException e1) {
                throw new TTransportException(e1);
            }

            try {
                sslContext.init(null, null, null);
            }
            catch (KeyManagementException e1) {
                throw new TTransportException(e1);
            }

            final SSLSocketFactory factory = sslContext.getSocketFactory();
            try {
                websocket.setSocket(factory.createSocket());
            }
            catch (IOException e1) {
                throw new TTransportException(e1);
            }
        }

        websocket.connect();

        executor.submit(new Runnable() {

            @Override
            public void run() {
                try {
                    final Websocket s = connectFuture.get(connectTimeout, TimeUnit.MILLISECONDS);
                    log.trace("Handshake completed, sessionId={}", s.toString());
                }
                catch (TimeoutException e) {
                    log.debug("Wait for connect:", e);
                    fireOnConnectError();
                    close();
                }
                catch (InterruptedException e) {
                    log.debug("Wait for connect", e);
                    fireOnConnectError();
                    close();
                }
                catch (ExecutionException e) {
                    log.debug("Wait for connect", e);
                    fireOnConnectError();
                    close();
                }

            }
        });
    }

    public void waitForConnect() throws TTransportException {
        try {
            final Websocket s = connectFuture.get(connectTimeout, TimeUnit.MILLISECONDS);
            log.trace("Handshake completed, websocket={}", s.toString());
        }
        catch (TimeoutException e) {
            log.debug("TimeoutException");
            close();
            throw new TTransportException(TTransportException.TIMED_OUT, "timed out");
        }
        catch (InterruptedException e) {
            close();
            throw new TTransportException(e);
        }
        catch (ExecutionException e) {
            close();
            if (e.getCause() != null && e.getCause() instanceof TTransportException)
                throw (TTransportException) e.getCause();
            else
                throw new TTransportException(e);
        }
    }

    /**
     * Нельзя вызывать из потоков websocket, т.к. тут закрывается client
     */
    @Override
    public final void close() {

        log.trace("close()");

        final boolean s = connectFuture.setException(new TTransportException("connect error"));

        log.trace("settings exception for connectFuture: {}", s);

        final TransportEventsIF h;

        synchronized (this) {
            if (closed) {
                log.trace("allready closed");
                return;
            }

            closed = true;

            if (websocket != null) {
                try {
                    websocket.closeBlocking();
                }
                catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }

            try {
                queue.put(EOF);
            }
            catch (InterruptedException e) {
                log.error("InterruptedException", e);
            }

            for (Integer i : pendingRequests) {
                log.debug("Throw TTransportException for call seqId={}", i);
                final InvocationInfo<?> ii = async.pop(i);
                if (ii != null)
                    ii.setException(new TTransportException(TTransportException.END_OF_FILE, "closed"));
            }

            pendingRequests.clear();

            if (isConnected) {
                isConnected = false;

                h = eventsHandler;
            } else {
                h = null;
            }
        }

        if (h != null)
            h.onClose();

    }

    /**
     * Без синхронизации во избежании возможности дедлока
     */
    private void fireOnConnect() {

        log.trace("fireOnConnect() : {}", websocket);

        final TransportEventsIF h;

        synchronized (this) {
            if (isConnected == false && closed == false) {
                isConnected = true;

                h = eventsHandler;
            } else {
                h = null;
            }
        }

        final boolean s = connectFuture.set(websocket);

        log.trace("fireOnConnect() connectFuture.set : {}", s);

        if (h != null)
            h.onConnect();
    }

    private void fireOnConnectError() {

        final TransportEventsIF h;

        synchronized (this) {
            h = eventsHandler;
        }

        if (h != null)
            h.onConnectError();
    }

    @Override
    public int read(byte[] buf, final int off, final int len) throws TTransportException {

        if (len == 0)
            return 0;

        synchronized (this) {
            if (async != null)
                throw new UnsupportedOperationException();

            if (!isConnected)
                throw new TTransportException(TTransportException.NOT_OPEN, "closed");
        }

        int read = 0;

        if (pendingBytes != null) {
            final int pendingBytesLen = pendingBytes.length - pendingBytesPos;
            final int l = pendingBytesLen <= len ? pendingBytesLen : len;
            System.arraycopy(pendingBytes, pendingBytesPos, buf, off, l);
            pendingBytesPos += l;
            read += l;

            if (pendingBytesPos == pendingBytes.length) {
                pendingBytes = null;
                pendingBytesPos = 0;
            }

            if (read == len)
                return len;
        }

        while (read < len) {

            ByteBuffer b;
            try {
                b = queue.poll(1000, TimeUnit.MILLISECONDS);
            }
            catch (InterruptedException e) {
                throw new TTransportException(e);
            }

            if (b == EOF)
                throw new TTransportException("No more data available.");

            if (b == null) {
                synchronized (this) {
                    if (!isConnected)
                        throw new TTransportException(TTransportException.NOT_OPEN, "closed");
                }
                continue;
            }

            final byte bb[] = b.array();
            final int bb_off = b.position();
            final int bb_len = b.limit() - b.position();

            final int l = bb_len <= len - read ? bb_len : len - read;
            System.arraycopy(bb, bb_off, buf, off + read, l);
            read += l;

            if (l < bb_len) {
                pendingBytes = bb;
                pendingBytesPos = bb_off + l;
            }
        }

        return len;
    }

    @Override
    public void write(byte[] buf, int off, int len) throws TTransportException {
        requestBuffer_.get().write(buf, off, len);
    }

    @Override
    public void flush(int seqId) throws TTransportException {

        synchronized (this) {
            if (async != null)
                pendingRequests.add(seqId);
        }

        flush();
    }

    @Override
    public void flush() throws TTransportException {

        final ByteArrayOutputStream rb = requestBuffer_.get();
        final byte[] data = rb.toByteArray();
        rb.reset();

        synchronized (this) {
            if (!isConnected)
                throw new TTransportException(TTransportException.NOT_OPEN, "closed");

            try {
                websocket.send(data);
            }
            catch (NotYetConnectedException e) {
                close();
                new TTransportException(e);
            }
        }
    }

    private void onReadReply(TMessage msg, TTransport in, byte buf[], int offset, int length) throws IOException {

        final AsyncRegister async;

        synchronized (this) {
            async = this.async;
        }

        if (async == null) {
            queue.add(ByteBuffer.wrap(buf, offset, length));
        } else {
            final InvocationInfo<?> ii = async.pop(msg.seqid);
            if (ii == null) {
                log.warn("Callback for seqId={} not found", msg.seqid);
            } else {
                try {
                    synchronized (this) {
                        pendingRequests.remove(msg.seqid);
                    }
                    ii.setReply(in, protocolFactory);
                }
                catch (TException e) {

                }
            }
        }
    }

    private synchronized void onReadRequest(TMessage msg, TTransport inWrapT) throws TException {

        TMemoryBuffer outT = null;
        TTransport outWrapT = null;

        try {
            outT = new TMemoryBuffer(1024);
            outWrapT = transportFactory.getTransport(outT);

            final TProtocol inP = protocolFactory.getProtocol(inWrapT);
            final TProtocol outP = protocolFactory.getProtocol(outWrapT);

            if (processor != null) {
                processor.process(inP, outP);
            } else {
                inP.readMessageBegin();
                TProtocolUtil.skip(inP, TType.STRUCT);
                inP.readMessageEnd();
                TApplicationException x = new TApplicationException(TApplicationException.UNSUPPORTED_CLIENT_TYPE,
                                                                    "Invalid method name: '" + msg.name + "'");
                outP.writeMessageBegin(new TMessage(msg.name, TMessageType.EXCEPTION, msg.seqid));
                x.write(outP);
                outP.writeMessageEnd();
                outP.getTransport().flush();
            }

            try {
                websocket.send(outT.toByteArray());
            }
            catch (Exception e) {
                new TTransportException(e);
            }
        }
        finally {

            if (outWrapT != null)
                outWrapT.close();

            if (outT != null)
                outT.close();
        }
    }

    private void onRead(byte buf[], int offset, int length) throws TException, IOException {
        final TMemoryInputTransport inT = new TMemoryInputTransport(buf, offset, length);
        TTransport inWrapT = null;
        try {
            inWrapT = transportFactory.getTransport(inT);

            final TMessage msg = protocolFactory.getProtocol(inWrapT).readMessageBegin();

            final TMemoryInputTransport copy = new TMemoryInputTransport(inWrapT.getBuffer(), 0,
                                                                         inWrapT.getBufferPosition() + inWrapT.getBytesRemainingInBuffer());

            if (msg.type == TMessageType.EXCEPTION || msg.type == TMessageType.REPLY) {
                onReadReply(msg, copy, buf, offset, length);
            } else {
                onReadRequest(msg, copy);
            }
        }
        finally {
            if (inWrapT != null)
                inWrapT.close();

            inT.close();
        }
    }

    private class Websocket extends WebSocketClient {

        public Websocket(URI uri) {
            super(uri, new Draft_17());
        }

        @Override
        public void onOpen(ServerHandshake handshakedata) {

            log.trace("onOpen: {}", handshakedata);

            try {
                executor.submit(new Runnable() {

                    @Override
                    public void run() {
                        TWsTransport.this.fireOnConnect();
                    }
                });

            }
            catch (RejectedExecutionException e) {

            }
        }

        @Override
        public void onMessage(String message) {
            final byte[] buf = message.getBytes(UTF_8);

            executor.execute(new Runnable() {

                @Override
                public void run() {
                    try {
                        onRead(buf, 0, buf.length);
                    }
                    catch (Exception e) {
                        log.error("onMessage", e);
                    }
                }
            });
        }

        @Override
        public void onMessage(final ByteBuffer bytes) {

            executor.execute(new Runnable() {

                @Override
                public void run() {
                    try {
                        onRead(bytes.array(), 0, bytes.array().length);
                    }
                    catch (Exception e) {
                        log.error("onMessage", e);
                    }
                }
            });
        }

        @Override
        public void onError(Exception ex) {
            log.error("onError", ex);

            executor.execute(new Runnable() {

                @Override
                public void run() {
                    TWsTransport.this.close();
                }
            });
        }

        @Override
        public void onClose(int code, String reason, boolean remote) {
            log.trace("onClose, code={}, reason={}", code, reason);

            executor.execute(new Runnable() {

                @Override
                public void run() {
                    TWsTransport.this.close();
                }
            });
        }
    }

    @Override
    public synchronized void setEventsHandler(TransportEventsIF eventsHandler) {
        this.eventsHandler = eventsHandler;
    }
}
