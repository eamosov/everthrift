package org.everthrift.thrift;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

public aspect TBaseLazyAspect {

    private static final Logger log = LoggerFactory.getLogger(TBaseLazyModel.class);

    public pointcut rwObject(): execution(void writeObject(java.io.ObjectOutputStream)) || execution(void readObject(java.io.ObjectInputStream));

    public pointcut tBaseExclude(): execution(void read(byte[])) || execution(byte[] write()) || execution(void readExternal(ObjectInput)) ||
        execution(void writeExternal(ObjectOutput)) || execution(void clear()) || execution(void deepCopyFields(..)) ||
        execution(* deepCopy()) || execution(String toString()) || execution(void unpack()) || execution(void pack()) ||
        execution(void read(org.apache.thrift.protocol.TProtocol)) || execution(void write(org.apache.thrift.protocol.TProtocol)) || rwObject() ||
        execution(boolean equals(..)) || execution(Logger getLogger()) || execution(byte[] getThriftData()) || execution(void setThriftData(byte[])) ||
        execution(* fieldForId(int));

    public byte[] TBaseLazyModel.thrift_data = null;

    public byte[] TBaseLazyModel.getThriftData() {
        return thrift_data;
    }

    public void TBaseLazyModel.setThriftData(byte[] bytes) {
        thrift_data = bytes;
    }

    Object around(TBaseLazyModel acc): this(acc) && execution(String TBaseHasLazyModel+.toString()){

        final byte[] bytes = acc.getThriftData();
        if (bytes != null) {
            return String.format("%s(<compressed>, length:%d)", thisJoinPointStaticPart.getSignature()
                                                                                       .getDeclaringType()
                                                                                       .getName(), bytes.length);
        } else {
            return proceed(acc);
        }
    }

    before(TBaseLazyModel acc): this(acc) && execution(void TBaseHasLazyModel+.clear()){
        acc.setThriftData(null);
    }

    Object around(TBaseLazyModel acc, TBaseLazyModel that): this(acc) && execution(boolean TBaseHasLazyModel+.equals(TBase+)) && args(that){

        if (log.isTraceEnabled()) {
            final String s1 = acc.getThriftData() == null ? acc.toString() : String.format("%s(<compressed>, length:%d)", acc
                .getClass()
                .getSimpleName(), acc.getThriftData().length);
            final String s2 = that.getThriftData() == null ? that.toString() : String.format("%s(<compressed>, length:%d)", that
                .getClass()
                .getSimpleName(), that.getThriftData().length);
            log.trace("equals {}({}) {}({})", System.identityHashCode(acc), s1, System.identityHashCode(that), s2);
        }

        if (that == null) {
            return false;
        }

        final byte[] acc_bytes = acc.getThriftData();
        final byte[] that_bytes = that.getThriftData();

        if (acc_bytes != null && that_bytes != null && (acc_bytes == that_bytes || Arrays.equals(acc_bytes, that_bytes))) {
            return true;
        }

        return proceed(acc.asUnpacked(), that.asUnpacked());
    }

    Object around(TBaseLazyModel acc, TBaseLazyModel other): this(acc) && execution(void TBaseHasLazyModel+.deepCopyFields(..)) && args(other) {

        final byte[] _data = other.getThriftData();

        if (_data == null) {
            return proceed(acc, other);
        } else {
            acc.clear();
            acc.setThriftData(_data);
            return null;
        }
    }

    //TODO убрать зависимость от com.knockchat.knock.thrift
    Object around(TBaseLazyModel acc): this(acc) && execution(* (TBaseHasLazyModel+).*(..)) && within(com.knockchat.knock.thrift..*) && !TBaseLazyAspect.tBaseExclude() && !cflow(adviceexecution() && within(TBaseLazyAspect)) && !cflow(within(TBaseLazyModel)) {

        if (log.isTraceEnabled()) {
            log.trace("around: {}", thisJoinPoint.toShortString());
        }

        acc.unpack();
        return proceed(acc);
    }

}
