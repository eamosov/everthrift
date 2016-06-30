package org.everthrift.appserver.model;

import org.apache.thrift.TException;
import org.everthrift.thrift.TFunction;

public abstract class BasicMutator<ENTITY> implements TFunction<ENTITY, Boolean>{

    @Override
    public Boolean apply(ENTITY input) throws TException {
        return update(input);
    }

    public abstract boolean update(ENTITY input) throws TException;
}