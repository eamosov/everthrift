package org.everthrift.thrift;

import org.apache.thrift.TException;

public interface TFunction<F, T> {
    T apply(F input) throws TException;
}
