package com.knockchat.utils.thrift;

import java.util.Arrays;

import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.TFieldIdEnum;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.transport.AutoExpandingBufferWriteTransport;
import org.apache.thrift.transport.TMemoryInputTransport;

import com.google.common.base.Throwables;

public interface TBaseModel<T extends TBase<?,?>, F extends TFieldIdEnum> extends TBase<T,F>{

	default byte[] write(){
		try{
			final AutoExpandingBufferWriteTransport t = new AutoExpandingBufferWriteTransport(1024, 1.5);
			
//			synchronized(this){				
				write(new TCompactProtocol(t));						
//			}
			return Arrays.copyOf(t.getBuf().array(), t.getPos()); 
		}catch (TException e){
			throw Throwables.propagate(e);
		}
	}
	
	default void read(byte[] in){
		try{
	//		synchronized(this){
				clear();
				read(new TCompactProtocol(new TMemoryInputTransport(in)));			
//			}			
		}catch (TException e){
			throw Throwables.propagate(e);
		}		
	}

}
