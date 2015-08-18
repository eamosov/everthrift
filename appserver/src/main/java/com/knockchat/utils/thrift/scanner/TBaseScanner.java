package com.knockchat.utils.thrift.scanner;

import org.apache.thrift.TBase;

public interface TBaseScanner {
	void scan(TBase o, TBaseScanHandler h);
	String getGeneratedCode();
}
