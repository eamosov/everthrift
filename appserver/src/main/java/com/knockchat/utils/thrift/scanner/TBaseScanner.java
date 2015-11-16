package com.knockchat.utils.thrift.scanner;

import com.knockchat.appserver.model.lazy.Registry;

public interface TBaseScanner {
	void scan(Object o, TBaseScanHandler h, Registry r);
	String getGeneratedCode();
}
