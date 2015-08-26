package com.knockchat.utils.thrift;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;

import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TTupleProtocol;
import org.apache.thrift.transport.TMemoryBuffer;
import org.apache.thrift.transport.TMemoryInputTransport;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.reflect.CodeSignature;
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
	

	public byte[] TBaseLazy.thrift_data = null;
	
	private static byte[] toByteArray(TBase o) throws TException{
		final TMemoryBuffer t = new TMemoryBuffer(100);
		o.write(new TTupleProtocol(t));
		return t.toByteArray();		
	}
	
	public static <T extends TBaseLazy> T asUnpacked(T t){
		if (t.thrift_data == null){
			return t;
		}else{
			final T other = (T)((TBase)t).deepCopy();
			other.unpack();
			return other;
		}
	}
		
	public byte[] TBaseLazy.write() throws TException{
				
		if (this.thrift_data !=null){
			return this.thrift_data;
		}else{
			return toByteArray((TBase)this);
		}		
	}
	
	public void TBaseLazy.read(byte[] in) throws TException{
		((TBase)this).clear();
		this.thrift_data = Arrays.copyOf(in, in.length);
	}
		
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
	
	public void TBaseLazy.readExternal(final ObjectInput in) throws IOException, ClassNotFoundException {
        // it doesn't seem like you should have to do this, but java serialization is wacky, and doesn't call the default constructor.
		((TBase)this).clear();
    	final int l = in.readInt();
    	final byte[] _data = new byte[l];
    	in.read(_data, 0, l);
    	this.thrift_data = _data;
	}
	
	public void TBaseLazy.unpack(){
		if (this.thrift_data !=null){
			
			if (log.isDebugEnabled())
				log.debug("Unpack object {} of type {}", System.identityHashCode(this), this.getClass().getSimpleName());
	    	  
			byte []_data = this.thrift_data;
			this.thrift_data = null;
			try{
				((TBase)this).read(new TTupleProtocol(new TMemoryInputTransport(_data)));		
			}catch(TException e){
				throw new RuntimeException(e);
			}
		}		
	}

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
