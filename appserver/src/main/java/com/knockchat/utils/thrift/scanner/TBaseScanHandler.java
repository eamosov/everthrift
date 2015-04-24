package com.knockchat.utils.thrift.scanner;

import org.apache.thrift.TBase;

public interface TBaseScanHandler {
	void apply(TBase o);
}
