package org.everthrift.appserver.model;

import org.apache.thrift.TException;
import org.jetbrains.annotations.NotNull;

public interface EntityFactory<PK, ENTITY> {
    @NotNull
    ENTITY create(PK id) throws TException;

    static <PK, ENITY> EntityFactory<PK, ENITY> of(EntityFactory<PK, ENITY> f) {
        return f;
    }
}