package org.everthrift.appserver.utils.thrift.scanner;

import org.everthrift.appserver.model.lazy.Registry;
import org.jetbrains.annotations.NotNull;

public interface TBaseScanner {
    void scan(Object parent, Object o, TBaseScanHandler h, Registry r);

    @NotNull
    String getGeneratedCode();
}
