package com.knockchat.appserver.transport;

import java.io.IOException;
import java.io.ObjectInput;

import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

public class TObjectInputTransport extends TTransport {

	final ObjectInput in;
	
	public TObjectInputTransport(ObjectInput in) {
		this.in = in;
	}

	@Override
	public boolean isOpen() { return true;}

	@Override
	public void open() throws TTransportException {	}

	@Override
	public void close() {}

	@Override
	public int read(byte[] buf, int off, int len) throws TTransportException {
		try {
			return in.read(buf, off, len);
		} catch (IOException e) {
			throw new TTransportException(e);
		}
	}

	@Override
	public void write(byte[] buf, int off, int len)throws TTransportException {
	}
}	
