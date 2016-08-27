package org.everthrift.clustering.thrift;

import com.google.common.util.concurrent.AbstractFuture;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.TFieldIdEnum;
import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.protocol.TMessage;
import org.apache.thrift.protocol.TMessageType;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TMemoryBuffer;
import org.apache.thrift.transport.TMemoryInputTransport;
import org.apache.thrift.transport.TTransport;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.CompletableFuture;

@SuppressWarnings("rawtypes")
public class InvocationInfo<T> extends CompletableFuture<T> {
    public final String fullMethodName;

    public final String serviceName;

    public final String methodName;

    public final TBase args;

    public final Constructor<? extends TBase> resultInit;

    public final AsyncMethodCallback asyncMethodCallback;

    private int seqId;

    public InvocationInfo(InvocationInfo other) {
        super();
        this.fullMethodName = other.fullMethodName;
        this.serviceName = other.serviceName;
        this.methodName = other.methodName;
        this.args = other.args;
        this.resultInit = other.resultInit;
        this.asyncMethodCallback = other.asyncMethodCallback;
    }

    public InvocationInfo(String fullMethodName, TBase args, Constructor<? extends TBase> resultInit) {
        super();
        this.fullMethodName = fullMethodName;
        final String[] m = fullMethodName.split(":");
        this.serviceName = m[0];
        this.methodName = m[1];
        this.args = args;
        this.resultInit = resultInit;
        this.asyncMethodCallback = null;
    }

    public InvocationInfo(String serviceName, String methodName, TBase args, Constructor<? extends TBase> resultInit,
                          AsyncMethodCallback asyncMethodCallback) {
        super();
        this.fullMethodName = serviceName + ":" + methodName;
        this.serviceName = serviceName;
        this.methodName = methodName;
        this.args = args;
        this.resultInit = resultInit;
        this.asyncMethodCallback = asyncMethodCallback;
    }

    public void buildCall(int seqId, TTransport outT, TProtocolFactory protocolFactory) {
        this.seqId = seqId;

        final TProtocol outProtocol = protocolFactory.getProtocol(outT);

        try {
            outProtocol.writeMessageBegin(new TMessage(fullMethodName, TMessageType.CALL, seqId));
            args.write(outProtocol);
            outProtocol.writeMessageEnd();
            outProtocol.getTransport().flush();
        } catch (TException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * @param seqId
     * @param protocolFactory
     * @return Transform to array:
     * <p>
     * payload.array(), payload.position(), payload.limit() - payload.position()
     */
    public TMemoryBuffer buildCall(int seqId, TProtocolFactory protocolFactory) {

        final TMemoryBuffer outT = new TMemoryBuffer(1024);
        buildCall(seqId, outT, protocolFactory);
        return outT;
    }

    public T setReply(byte[] data, TProtocolFactory protocolFactory) throws TException {
        return setReply(data, 0, data.length, protocolFactory);
    }

    public T setReply(byte[] data, int offset, int length, TProtocolFactory protocolFactory) throws TException {
        return setReply(new TMemoryInputTransport(data, offset, length), protocolFactory);
    }

    @SuppressWarnings("unchecked")
    public T setReply(TTransport inT, TProtocolFactory protocolFactory) throws TException {
        try {
            final T ret = (T) this.parseReply(inT, protocolFactory);
            super.complete(ret);
            return ret;
        } catch (TException e) {
            super.completeExceptionally(e);
            throw e;
        }
    }

    public void setException(TException e) {
        super.completeExceptionally(e);
    }

    @SuppressWarnings("unchecked")
    private Object parseReply(TTransport inT, TProtocolFactory protocolFactory) throws TException {
        final TProtocol inProtocol = protocolFactory.getProtocol(inT);

        TMessage msg = inProtocol.readMessageBegin();
        if (msg.type == TMessageType.EXCEPTION) {
            TApplicationException x = new TApplicationException();
            x.read(inProtocol);
            inProtocol.readMessageEnd();
            throw x;
        }

        if (msg.type != TMessageType.REPLY) {
            throw new TApplicationException(TApplicationException.INVALID_MESSAGE_TYPE, this.fullMethodName + " failed: invalid msg type");
        }

        if (msg.seqid != seqId) {
            throw new TApplicationException(TApplicationException.BAD_SEQUENCE_ID, fullMethodName + " failed: out of sequence response");
        }

        if (!msg.name.equals(this.fullMethodName)) {
            throw new TApplicationException(TApplicationException.WRONG_METHOD_NAME,
                                            fullMethodName + " failed: invalid method name '" + msg.name + "' in reply. Need '"
                                                + this.fullMethodName + "'");
        }

        final TBase result;
        try {
            result = resultInit.newInstance();
        } catch (IllegalArgumentException e) {
            throw new RuntimeException(e);
        } catch (InstantiationException e) {
            throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        } catch (InvocationTargetException e) {
            throw new RuntimeException(e);
        }

        result.read(inProtocol);
        inProtocol.readMessageEnd();

        Object o = null;
        int i = 1;
        do {// Пытаемся найти exception
            final TFieldIdEnum f = result.fieldForId(i++);
            if (f == null) {
                break;
            }

            o = result.getFieldValue(f);
            if (o != null) {
                break;
            }
        } while (o == null);

        if (o == null) {// Пробуем прочитать success
            final TFieldIdEnum f = result.fieldForId(0);
            if (f != null) {
                o = result.getFieldValue(f);
            }
        }

        if (o == null) {
            return null;
        }

        if (o instanceof RuntimeException) {
            throw (RuntimeException) o;
        } else if (o instanceof TException) {
            throw (TException) o;
        }

        return o;
    }

    @Override
    public String toString() {
        return "InvocationInfo [" + fullMethodName + "(" + args + "), seqId=" + seqId + "]";
    }
}