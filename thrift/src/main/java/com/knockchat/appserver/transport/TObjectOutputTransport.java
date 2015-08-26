package com.knockchat.appserver.transport;

import java.io.IOException;
import java.io.ObjectOutput;

import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

public class TObjectOutputTransport extends TTransport {

	private final ObjectOutput out;
	
	public TObjectOutputTransport(ObjectOutput out) {
		this.out =out;
	}
	
	@Override
	public boolean isOpen() {
		return true;
	}

	@Override
	public void open() throws TTransportException { }

	@Override
	public void close() {}

	@Override
	public int read(byte[] buf, int off, int len) throws TTransportException {
		return 0;
	}

	@Override
	public void write(byte[] buf, int off, int len) throws TTransportException {
		try {
			out.write(buf, off, len);
		} catch (IOException e) {
			throw new TTransportException(e);
		}
	}
}
