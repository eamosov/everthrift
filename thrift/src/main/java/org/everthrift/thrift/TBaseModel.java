package org.everthrift.thrift;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;

import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.TFieldIdEnum;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.transport.AutoExpandingBufferWriteTransport;
import org.apache.thrift.transport.TMemoryInputTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Throwables;

import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;

public interface TBaseModel<T extends TBase<T,F>, F extends TFieldIdEnum> extends TBase<T,F>, java.io.Externalizable{

    static final Logger log = LoggerFactory.getLogger(TBaseLazyModel.class);

    final static LZ4Factory factory = LZ4Factory.fastestInstance();
    final static LZ4Compressor compressor = factory.fastCompressor();
    final static LZ4FastDecompressor decompressor = factory.fastDecompressor();

    static void encodeFrameSize(final int frameSize, final byte[] buf) {
        buf[3] = (byte)(0xff & (frameSize >> 24));
        buf[2] = (byte)(0xff & (frameSize >> 16));
        buf[1] = (byte)(0xff & (frameSize >> 8));
        buf[0] = (byte)(0xff & (frameSize));
    }

    static int decodeFrameSize(final byte[] buf, int offset) {
        return  ((buf[offset+3] & 0xff) << 24) | ((buf[offset+2] & 0xff) << 16) | ((buf[offset+1] & 0xff) <<  8) | ((buf[offset+0] & 0xff));
    }

    public static byte[] toByteArray(TBase tBase){
        try{
            final AutoExpandingBufferWriteTransport t = new AutoExpandingBufferWriteTransport(1024, 1.5);
            tBase.write(new TCompactProtocol(t));

            final int decompressedLength = t.getPos();
            final int maxCompressedLength = compressor.maxCompressedLength(decompressedLength);
            final byte[] compressed = new byte[maxCompressedLength+4];
            final int compressedLength = compressor.compress(t.getBuf().array(), 0, decompressedLength, compressed, 4, maxCompressedLength);

            log.debug("toByteArray: decompressedLength={}, compressedLength={}", decompressedLength, compressedLength);

            encodeFrameSize(decompressedLength, compressed);

            if (compressedLength == maxCompressedLength)
                return compressed;
            else
                return Arrays.copyOf(compressed, compressedLength+4);
        }catch (TException e){
            throw Throwables.propagate(e);
        }
    }

    default byte[] toByteArray(){
        return toByteArray(this);
    }

    public static void fromByteArray(TBase tBase, byte []_data){
        fromByteArray(tBase, _data, 0);
    }

    public static void fromByteArray(TBase tBase, byte []_data, int offset){

        final int decompressedLength = decodeFrameSize(_data, offset);

        try{
            final byte[] restored = new byte[decompressedLength];
            decompressor.decompress(_data, offset + 4, restored, 0, decompressedLength);
            tBase.read(new TCompactProtocol(new TMemoryInputTransport(restored)));
        }catch (Exception e){
            log.error("fromByteArray: _data.length={}, offset={}, decompressedLength={}, _data={}", _data.length, offset, decompressedLength, _data);
            throw Throwables.propagate(e);
        }
    }

    default void fromByteArray(byte []_data){
        fromByteArray(this, _data, 0);
    }

    default void fromByteArray(byte []_data, int offset){
        fromByteArray(this, _data, offset);
    }

    default byte[] write(){
        return toByteArray();
    }

    default void read(byte[] in, int offset){
        clear();
        fromByteArray(in, offset);
    }

    @Override
    default void writeExternal(final ObjectOutput out) throws IOException {
        final byte[] _data = toByteArray();
        out.writeInt(_data.length);
        out.write(_data);
    }

    @Override
    default void readExternal(final ObjectInput in) throws IOException, ClassNotFoundException {

        final int l = in.readInt();
        final byte[] _data = new byte[l];
        in.readFully(_data, 0, l);

        clear();
        fromByteArray(_data, 0);
    }

}
