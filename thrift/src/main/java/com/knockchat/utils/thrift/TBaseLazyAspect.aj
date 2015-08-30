package com.knockchat.utils.thrift;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;

import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;

import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TTupleProtocol;
import org.apache.thrift.transport.AutoExpandingBufferWriteTransport;
import org.apache.thrift.transport.TMemoryInputTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public aspect TBaseLazyAspect {
				
	private static final Logger log = LoggerFactory.getLogger(TBaseLazy.class); 

	public pointcut rwObject(): execution(void writeObject(java.io.ObjectOutputStream)) || execution(void readObject(java.io.ObjectInputStream));

	public pointcut tBaseExclude(): execution(void read(byte[])) || execution(byte[] write()) || execution(void readExternal(ObjectInput)) || 
					execution(void writeExternal(ObjectOutput)) || execution(void clear()) || execution(void deepCopyFields(..)) ||
					execution(* deepCopy()) || execution(String toString()) || execution(void unpack()) || execution(void pack()) ||
					execution(void read(org.apache.thrift.protocol.TProtocol)) || execution(void write(org.apache.thrift.protocol.TProtocol)) || rwObject() ||
					execution(boolean equals(..));
	
	private final static LZ4Factory factory = LZ4Factory.fastestInstance();	
	private final static LZ4Compressor compressor = factory.fastCompressor();
	private final static LZ4FastDecompressor decompressor = factory.fastDecompressor();

	public byte[] TBaseLazy.thrift_data = null;
	
	private static final void encodeFrameSize(final int frameSize, final byte[] buf) {
		buf[0] = (byte)(0xff & (frameSize >> 24));
		buf[1] = (byte)(0xff & (frameSize >> 16));
		buf[2] = (byte)(0xff & (frameSize >> 8));
		buf[3] = (byte)(0xff & (frameSize));
	}

	private static final int decodeFrameSize(final byte[] buf) {
		return  ((buf[0] & 0xff) << 24) | ((buf[1] & 0xff) << 16) | ((buf[2] & 0xff) <<  8) | ((buf[3] & 0xff));
	}

	@SuppressWarnings("rawtypes")
	private static byte[] toByteArray(TBase o) throws TException{
		final AutoExpandingBufferWriteTransport t = new AutoExpandingBufferWriteTransport(1024, 1.5);
		o.write(new TTupleProtocol(t));
				
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
	}
	
	@SuppressWarnings("rawtypes")
	private static void fromByteArray(TBase o, byte []_data) throws TException{
		
		final int decompressedLength = decodeFrameSize(_data);
		final byte[] restored = new byte[decompressedLength];
		final int compressedLength2 = decompressor.decompress(_data, 4, restored, 0, decompressedLength);
		
		log.debug("fromByteArray: compressedLength={} decompressedLength={}", _data.length-4, decompressedLength);
		
		if (compressedLength2 != _data.length - 4)
			throw new TException("Decompress LZ4 error: compressedLength=" + (_data.length - 4) + " compressedLength2=" + compressedLength2);
		
		o.read(new TTupleProtocol(new TMemoryInputTransport(restored)));		
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static <T extends TBaseLazy> T asUnpacked(T t){
		if (t.thrift_data == null){
			return t;
		}else{
			final T other = (T)((TBase)t).deepCopy();
			other.unpack();
			return other;
		}
	}
	
	@SuppressWarnings({ "rawtypes" })
	public byte[] TBaseLazy.write() throws TException{
				
		if (this.thrift_data !=null){
			return this.thrift_data;
		}else{
			return toByteArray((TBase)this);
		}		
	}
	
	@SuppressWarnings({ "rawtypes"})
	public void TBaseLazy.read(byte[] in) throws TException{
		((TBase)this).clear();
		this.thrift_data = Arrays.copyOf(in, in.length);
	}
	
	@SuppressWarnings({ "rawtypes" })
	public void TBaseLazy.writeExternal(final ObjectOutput out) throws IOException {
		if (this.thrift_data !=null){
			out.writeInt(this.thrift_data.length);
			out.write(this.thrift_data);
		}else{
			try {
				final byte[] _data = toByteArray((TBase)this);
				out.writeInt(_data.length);
				out.write(_data);
			} catch (org.apache.thrift.TException te) {
				throw new java.io.IOException(te);
			}				
		}
	}
	
	@SuppressWarnings({ "rawtypes"})
	public void TBaseLazy.readExternal(final ObjectInput in) throws IOException, ClassNotFoundException {
        // it doesn't seem like you should have to do this, but java serialization is wacky, and doesn't call the default constructor.
		((TBase)this).clear();
    	final int l = in.readInt();
    	final byte[] _data = new byte[l];
    	in.read(_data, 0, l);
    	this.thrift_data = _data;
	}
	
	@SuppressWarnings({ "rawtypes"})
	public void TBaseLazy.unpack(){
		if (this.thrift_data !=null){
			
			if (log.isDebugEnabled())
				log.debug("Unpack object {} of type {}", System.identityHashCode(this), this.getClass().getSimpleName());
	    	  
			byte []_data = this.thrift_data;
			this.thrift_data = null;
			try{
				fromByteArray((TBase)this, _data);
			}catch(TException e){
				throw new RuntimeException(e);
			}
		}		
	}

	@SuppressWarnings({ "rawtypes"})
	public void TBaseLazy.pack(){
		if (this.thrift_data ==null){
			
			if (log.isDebugEnabled())
				log.debug("Pack object {} of type {}", System.identityHashCode(this), this.getClass().getSimpleName());
	    	
			try{
				byte[] _data = toByteArray((TBase)this);
				((TBase)this).clear();
				this.thrift_data = _data;
			}catch(TException e){
				throw new RuntimeException(e);
			}
		}		
	}
	
	public boolean TBaseLazy.isPacked(){
		return this.thrift_data != null;
	}

//	static private void printParameters(JoinPoint jp) {
//		println("Arguments: " );
//		Object[] args = jp.getArgs();
//		String[] names = ((CodeSignature)jp.getSignature()).getParameterNames();
//		Class[] types = ((CodeSignature)jp.getSignature()).getParameterTypes();
//		for (int i = 0; i < args.length; i++) {
//			println("  "  + i + ". " + names[i] + " : " +  types[i].getName() + " = " + args[i]);
//		}
//	}

}
