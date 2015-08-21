package com.knockchat.utils.thrift.scanner;

import org.apache.thrift.TBase;

import com.knockchat.appserver.model.lazy.Registry;

public interface TBaseScanner {
	void scan(TBase o, TBaseScanHandler h, Registry r);
	String getGeneratedCode();
}
