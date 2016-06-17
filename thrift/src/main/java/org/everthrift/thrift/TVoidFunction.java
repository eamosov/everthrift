package org.everthrift.thrift;

import org.apache.thrift.TException;

public interface TVoidFunction<F> {
	  void apply(F input) throws TException;
}
