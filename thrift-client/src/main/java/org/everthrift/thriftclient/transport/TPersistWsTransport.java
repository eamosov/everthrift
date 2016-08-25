package org.everthrift.thriftclient.transport;

import java.net.URI;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.transport.TTransportFactory;
import org.everthrift.thrift.AsyncRegister;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TPersistWsTransport extends TTransport {

    private static final Logger log = LoggerFactory.getLogger(TPersistWsTransport.class);

    final URI uri;

    final TProcessor processor;

    final TProtocolFactory protocolFactory;

    final TTransportFactory transportFactory;

    final AsyncRegister async;

    final long reconnectTimeoutMs;

    final long connectTimeoutMs;

    boolean opened = false;

    private TWsTransport ws;

    private Future<?> future;

    private final ScheduledExecutorService scheduller;

    private final ExecutorService executor;

    private final AtomicReference<TransportEventsIF> eventsHandler = new AtomicReference<TransportEventsIF>();

    private TransportEventsIF thisHandler = new TransportEventsIF() {

        @Override
        public void onConnect() {
            TPersistWsTransport.this._onConnect();
        }

        @Override
        public void onClose() {
            TPersistWsTransport.this._onClose();
        }

        @Override
        public void onConnectError() {
            TPersistWsTransport.this._onConnectError();
        }

    };

    public TPersistWsTransport(URI uri, TProcessor processor, TProtocolFactory protocolFactory, TTransportFactory transportFactory,
                               AsyncRegister async, ScheduledExecutorService scheduller, ExecutorService executor, long reconnectTimeoutMs,
                               long connectTimeoutMs) {
        this.uri = uri;
        this.processor = processor;
        this.protocolFactory = protocolFactory;
        this.transportFactory = transportFactory;
        this.async = async;
        this.scheduller = scheduller;
        this.executor = executor;
        this.reconnectTimeoutMs = reconnectTimeoutMs;
        this.connectTimeoutMs = connectTimeoutMs;
    }

    @Override
    public synchronized boolean isOpen() {
        return opened;
    }

    public synchronized boolean isConnected() {
        return opened && ws != null && ws.isOpen();
    }

    private synchronized TWsTransport setWs(TWsTransport ws) {

        final TWsTransport old = this.ws;

        if (ws != null) {
            this.ws = ws;
            this.ws.setEventsHandler(thisHandler);
        } else if (this.ws != null) {
            this.ws.setEventsHandler(null);
            this.ws = null;
        }

        return old;
    }

    @Override
    public synchronized void open() throws TTransportException {
        if (opened)
            return;

        opened = true;

        future = scheduleConnect(0);
    }

    private synchronized Future<?> scheduleConnect(final long reconnectTimeoutMs) {

        log.debug("schedulling connect in {} ms", reconnectTimeoutMs);

        final Runnable run = new Runnable() {

            @Override
            public void run() {
                doConnect();
            }
        };

        if (reconnectTimeoutMs == 0)
            return executor.submit(run);
        else
            return scheduller.schedule(run, reconnectTimeoutMs, TimeUnit.MILLISECONDS);

    }

    /**
     * Без синхронизации во избежании возможности дедлока
     */
    private void fireOnClose() {

        final TransportEventsIF h = eventsHandler.get();
        if (h != null)
            h.onClose();
    }

    /**
     * Без синхронизации во избежании возможности дедлока
     */
    private void fireOnConnect() {

        final TransportEventsIF h = eventsHandler.get();
        if (h != null)
            h.onConnect();
    }

    private void _onConnectError() {

        synchronized (this) {
            setWs(null);

            if (future == null || future.isDone())
                future = scheduleConnect(reconnectTimeoutMs);
        }

    }

    private void _onConnect() {

        synchronized (this) {
            log.info("onConnect");
        }

        fireOnConnect();
    }

    private final void _onClose() {

        synchronized (this) {
            setWs(null);

            if (future == null || future.isDone())
                future = scheduleConnect(reconnectTimeoutMs);

            log.info("onClose");
        }

        fireOnClose();
    }

    private synchronized boolean doConnect() {

        setWs(new TWsTransport(uri, connectTimeoutMs, processor, protocolFactory, transportFactory, async, executor));

        try {
            ws.openAsync();
        }
        catch (TTransportException e) {
            _onConnectError();
        }
        return true;
    }

    @Override
    public void close() {

        final boolean connected;

        synchronized (this) {
            if (!opened)
                return;

            connected = isConnected();

            opened = false;

            final TWsTransport old = setWs(null);
            if (old != null) {
                old.close();
            }

            future = null;
        }

        if (connected)
            fireOnClose();
    }

    @Override
    public synchronized int read(byte[] buf, int off, int len) throws TTransportException {

        if (!isConnected())
            throw new TTransportException(TTransportException.NOT_OPEN, "not connected");

        return ws.read(buf, off, len);
    }

    @Override
    public synchronized void write(byte[] buf, int off, int len) throws TTransportException {
        if (!isConnected())
            throw new TTransportException(TTransportException.NOT_OPEN, "not connected");

        ws.write(buf, off, len);
    }

    @Override
    public synchronized void flush() throws TTransportException {

        if (!isConnected())
            throw new TTransportException(TTransportException.NOT_OPEN, "not connected");

        ws.flush();
    }

    @Override
    public synchronized void flush(int seqId) throws TTransportException {

        if (!isConnected())
            throw new TTransportException(TTransportException.NOT_OPEN, "not connected");

        ws.flush(seqId);
    }

    public void setEventsHandler(TransportEventsIF eventsHandler) {
        this.eventsHandler.set(eventsHandler);
    }

}
