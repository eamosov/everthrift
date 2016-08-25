package org.everthrift.thrift;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import org.apache.thrift.TBase;
import org.apache.thrift.TFieldIdEnum;

public interface TBaseLazyModel<T extends TBase<T, F>, F extends TFieldIdEnum> extends TBaseModel<T, F> {

    byte[] getThriftData();

    void setThriftData(byte[] bytes);

    @SuppressWarnings({ "rawtypes", "unchecked" })
    default <T extends TBaseLazyModel> T asUnpacked() {

        final byte[] bytes = getThriftData();
        if (bytes == null)
            return (T) this;

        final T other = (T) newInstance();

        if (log.isTraceEnabled())
            log.trace("unpack object {} to new object {}", System.identityHashCode(this), System.identityHashCode(other));

        other.fromByteArray(bytes, 0);
        return other;
    }

    @Override
    default byte[] write() {
        final byte[] bytes = getThriftData();
        if (bytes != null) {
            return bytes;
        } else {
            return toByteArray();
        }
    }

    default void read(byte[] in) {
        clear();
        setThriftData(in);
    }

    @Override
    default void writeExternal(final ObjectOutput out) throws IOException {
        byte[] _data;

        if ((_data = getThriftData()) == null)
            _data = toByteArray();

        out.writeInt(_data.length);
        out.write(_data);
    }

    @Override
    default void readExternal(final ObjectInput in) throws IOException, ClassNotFoundException {

        final int l = in.readInt();
        final byte[] _data = new byte[l];
        in.readFully(_data, 0, l);

        clear();
        setThriftData(_data);
    }

    default void unpack() {
        final byte[] bytes = getThriftData();
        if (bytes != null) {

            if (log.isDebugEnabled())
                log.debug("Unpack object {} of type {}", System.identityHashCode(this), this.getClass().getSimpleName());

            setThriftData(null);
            fromByteArray(bytes, 0);
        }
    }

    default void pack() {
        if (this.getThriftData() == null) {

            if (log.isDebugEnabled())
                log.debug("Pack object {} of type {}", System.identityHashCode(this), this.getClass().getSimpleName());

            byte[] _data = toByteArray();
            clear();
            setThriftData(_data);
        }
    }

    default boolean isPacked() {
        return getThriftData() != null;
    }

}
