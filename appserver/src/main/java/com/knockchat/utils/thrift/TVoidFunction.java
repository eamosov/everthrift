package com.knockchat.utils.thrift;

import org.apache.thrift.TException;

public interface TVoidFunction<F> {
	  void apply(F input) throws TException;
}
