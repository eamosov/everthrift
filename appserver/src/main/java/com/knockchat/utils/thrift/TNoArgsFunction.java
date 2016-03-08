package com.knockchat.utils.thrift;

import org.apache.thrift.TException;

public interface TNoArgsFunction {
	void apply() throws TException;
}
