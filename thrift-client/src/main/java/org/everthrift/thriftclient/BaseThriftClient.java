package org.everthrift.thriftclient;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.thrift.TProcessor;
import org.apache.thrift.TServiceClient;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TJSONProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.THttpClient;
import org.apache.thrift.transport.TKnockZlibTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.transport.TTransportFactory;
import org.everthrift.thriftclient.transport.TWsTransport;

public class BaseThriftClient implements AutoCloseable{

    public static class HostPort{
        public final String addr;
        public final String host;
        public final int port;
        public final String descr;

        public HostPort(String addr, int port) {
            super();
            this.addr = addr;
            this.host = addr;
            this.port = port;
            this.descr = addr + ":" + port;
        }

        public HostPort(String addr, String host, int port, String descr) {
            super();
            this.addr = addr;
            this.host = host;
            this.port = port;
            this.descr = descr;
        }
    }


    public static enum Transports{
        HTTP,SOCKET, WEBSOCKET, WEBSOCKET_ZLIB, WEBSOCKET_SSL, WEBSOCKET_JS
    }

    private final TProtocol protocol;

    //	public Client( TProtocol protocol, HostPort hostPort ) {
    //		this.protocol = protocol;
    //		this.hostPort = hostPort;
    //	}

    //	public Client( TTransport transport, HostPort hostPort  ) {
    //		this( new TBinaryProtocol( transport ), hostPort );
    //	}

    static final AtomicInteger nThread = new AtomicInteger(0);

    private final static ExecutorService executor = new ThreadPoolExecutor(1, Integer.MAX_VALUE,
            5L, TimeUnit.SECONDS,
            new SynchronousQueue<Runnable>(),
            new ThreadFactory(){

        @Override
        public Thread newThread(Runnable r) {
            final Thread t = new Thread(r);
            t.setName("BaseThriftClientExecutor-" + nThread.incrementAndGet());
            t.setDaemon(true);
            return t;
        }});

    public static BaseThriftClient httpClient(String url) throws TTransportException{
        return new BaseThriftClient(url);
    }

    private BaseThriftClient(String url) throws TTransportException {
        final TTransport transport = new THttpClient(url);
        protocol = new TBinaryProtocol(transport);
        protocol.getTransport().open();
    }

    public static BaseThriftClient zlibWsClient(String url, TProcessor processor) throws TTransportException, URISyntaxException{
        return new BaseThriftClient(url, processor);
    }

    private BaseThriftClient(String url, TProcessor processor) throws TTransportException, URISyntaxException {
        final TTransport transport = new TWsTransport(new URI(url), 5000, processor, new  TBinaryProtocol.Factory(), new TKnockZlibTransport.Factory(), null, executor);
        protocol = new TBinaryProtocol(new TKnockZlibTransport(transport));
        protocol.getTransport().open();
    }

    public BaseThriftClient(HostPort hostPort, Transports proto, TProcessor processor) throws TTransportException {

        TTransport transport=null;

        if (proto.equals(Transports.SOCKET)){
            transport = new TFramedTransport(new TSocket( hostPort.addr, hostPort.port ));
            protocol = new TBinaryProtocol(transport);
        }else if (proto.equals(Transports.HTTP)){
            transport = new THttpClient("http://" + hostPort.addr + ":" + hostPort.port + "/");
            protocol = new TBinaryProtocol(transport);
        }else if (proto.equals(Transports.WEBSOCKET)){
            try {
                transport = new TWsTransport(new URI("ws://" + hostPort.addr + ":" + hostPort.port + "/thrift"), 5000, processor, new  TBinaryProtocol.Factory(), new TTransportFactory(), null, executor);
                protocol = new TBinaryProtocol(transport);
            } catch (URISyntaxException e) {
                throw new RuntimeException(e);
            }
        }else if (proto.equals(Transports.WEBSOCKET_ZLIB)){
            try {
                transport = new TWsTransport(new URI("ws://" + hostPort.addr + ":" + hostPort.port + "/thrift_zlib"), 5000, processor, new  TBinaryProtocol.Factory(), new TKnockZlibTransport.Factory(), null, executor);
                protocol = new TBinaryProtocol(new TKnockZlibTransport(transport));
            } catch (URISyntaxException e) {
                throw new RuntimeException(e);
            }
        }else if (proto.equals(Transports.WEBSOCKET_JS)){
            try {
                transport = new TWsTransport(new URI("ws://" + hostPort.addr + ":" + hostPort.port + "/thrift_js"), 5000, processor, new  TJSONProtocol.Factory(), new TTransportFactory(), null, executor);
                protocol = new TJSONProtocol(transport);
            } catch (URISyntaxException e) {
                throw new RuntimeException(e);
            }
        }else if (proto.equals(Transports.WEBSOCKET_SSL)){
            try {
                transport = new TWsTransport(new URI("wss://" + hostPort.addr + ":" + hostPort.port + "/thrift"), 5000, processor, new  TBinaryProtocol.Factory(), new TTransportFactory(), null, executor);
                protocol = new TBinaryProtocol(transport);
            } catch (URISyntaxException e) {
                throw new RuntimeException(e);
            }
        }else
            throw new TTransportException("unknown proto");

        protocol.getTransport().open();
    }

    public BaseThriftClient(HostPort hostPort) throws TTransportException {
        this( hostPort, Transports.SOCKET, null );
    }

    public <ClientType extends TServiceClient> ClientType getService( Class<ClientType> clientClass ) {
        try {
            ClientType client = clientClass.getConstructor( TProtocol.class ).newInstance( protocol );
            return client;
        } catch ( Throwable e ) {
            throw new RuntimeException( "Error opening service " + clientClass.getName(), e );
        }
    }

    public TProtocol getProtocol(){
        return this.protocol;
    }

    @Override
    public void close(){
        this.protocol.getTransport().close();
    }
}
