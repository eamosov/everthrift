package com.knockchat.utils.thrift;

import org.apache.thrift.TException;

public interface TBaseLazy extends java.io.Externalizable{
	
	void unpack();
	
	void pack();
	
	boolean isPacked();
	
	byte[] write() throws TException;
	
	void read(byte[] in) throws TException;

}
