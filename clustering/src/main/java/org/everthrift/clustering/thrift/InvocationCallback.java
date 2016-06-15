package org.everthrift.clustering.thrift;

import org.apache.thrift.TException;

public interface InvocationCallback{
	Object call(InvocationInfo ii) throws NullResult, TException;
}