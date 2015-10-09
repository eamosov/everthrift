package com.knockchat.utils.thrift;

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

public interface TBaseLazyModel<T extends TBase<?,?>, F extends TFieldIdEnum> extends TBaseModel<T,F>, java.io.Externalizable{
	
	static final Logger log = LoggerFactory.getLogger(TBaseLazyModel.class);
			
	final static LZ4Factory factory = LZ4Factory.fastestInstance();	
	final static LZ4Compressor compressor = factory.fastCompressor();
	final static LZ4FastDecompressor decompressor = factory.fastDecompressor();		
	
	byte[] getThriftData();
	void setThriftData(byte[] bytes);

	static void encodeFrameSize(final int frameSize, final byte[] buf) {
		buf[3] = (byte)(0xff & (frameSize >> 24));
		buf[2] = (byte)(0xff & (frameSize >> 16));
		buf[1] = (byte)(0xff & (frameSize >> 8));
		buf[0] = (byte)(0xff & (frameSize));
	}

	static int decodeFrameSize(final byte[] buf) {
		return  ((buf[3] & 0xff) << 24) | ((buf[2] & 0xff) << 16) | ((buf[1] & 0xff) <<  8) | ((buf[0] & 0xff));
	}
	
	default byte[] toByteArray(){
		try{
//			synchronized(this){
				final AutoExpandingBufferWriteTransport t = new AutoExpandingBufferWriteTransport(1024, 1.5);
				write(new TCompactProtocol(t));
						
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
//			}			
		}catch (TException e){
			throw Throwables.propagate(e);
		}
	}
	
	default void fromByteArray(byte []_data){
		try{
//			synchronized(this){
				final int decompressedLength = decodeFrameSize(_data);
				final byte[] restored = new byte[decompressedLength];
				final int compressedLength2 = decompressor.decompress(_data, 4, restored, 0, decompressedLength);
				
				log.debug("fromByteArray: compressedLength={} decompressedLength={}", _data.length-4, decompressedLength);
				
				if (compressedLength2 != _data.length - 4)
					throw new TException("Decompress LZ4 error: compressedLength=" + (_data.length - 4) + " compressedLength2=" + compressedLength2);
				
				read(new TCompactProtocol(new TMemoryInputTransport(restored)));					
//			}			
		}catch (TException e){
			throw Throwables.propagate(e);
		}
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	default <T extends TBaseLazyModel> T asUnpacked(){
		
		final byte [] bytes = getThriftData();
		if (bytes == null)
			return (T)this;
				
		final T other = (T)newInstance();

		if (log.isTraceEnabled())
			log.trace("unpack object {} to new object {}", System.identityHashCode(this), System.identityHashCode(other));

		other.fromByteArray(bytes);
		return other;		
	}

	default byte[] write(){
//		synchronized(this){
			final byte [] bytes = getThriftData();
			if (bytes !=null){
				return bytes;
			}else{
				return toByteArray();
			}					
//		}
	}
	
	default void read(byte[] in){
//		synchronized(this){
			clear();
			setThriftData(in);			
//		}
	}
	
	default void writeExternal(final ObjectOutput out) throws IOException {
		byte[] _data;
		
//		synchronized(this){
			if ((_data = getThriftData()) == null)
				_data = toByteArray();

//		}
		
		out.writeInt(_data.length);
		out.write(_data);		
	}
	
	default void readExternal(final ObjectInput in) throws IOException, ClassNotFoundException {
		
    	final int l = in.readInt();
    	final byte[] _data = new byte[l];
    	in.read(_data, 0, l);

//		synchronized(this){
			clear();
	    	setThriftData(_data);			
//		}
	}
	
	default void unpack(){
//		synchronized(this){
			final byte [] bytes = getThriftData();
			if (bytes !=null){
				
				if (log.isDebugEnabled())
					log.debug("Unpack object {} of type {}", System.identityHashCode(this), this.getClass().getSimpleName());
		    	  
				setThriftData(null);
				fromByteArray(bytes);
			}					
//		}
	}

	default void pack(){
//		synchronized(this){
			if (this.getThriftData() ==null){
				
				if (log.isDebugEnabled())
					log.debug("Pack object {} of type {}", System.identityHashCode(this), this.getClass().getSimpleName());
		    	
				byte[] _data = toByteArray();
				clear();
				setThriftData(_data);
			}					
//		}
	}
	
	default boolean isPacked(){
//		synchronized(this){
			return getThriftData() != null;			
//		}
	}
	
}
