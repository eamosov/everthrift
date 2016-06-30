package org.everthrift.appserver.utils.thrift.scanner;

import org.everthrift.appserver.model.lazy.Registry;

public interface TBaseScanner {
    void scan(Object parent, Object o, TBaseScanHandler h, Registry r);
    String getGeneratedCode();
}
