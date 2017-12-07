package org.everthrift.appserver.model;

import org.apache.thrift.TException;
import org.everthrift.appserver.model.events.DeleteEntityEvent;
import org.everthrift.appserver.model.events.InsertEntityEvent;
import org.everthrift.appserver.model.events.UpdateEntityEvent;
import org.jetbrains.annotations.NotNull;

import java.io.Serializable;

public interface RwModelFactoryIF<PK extends Serializable, ENTITY extends DaoEntityIF, E extends TException> extends RoModelFactoryIF<PK, ENTITY, E> {

    @NotNull
    ENTITY insertEntity(@NotNull ENTITY e) throws UniqueException;

    @NotNull
    ENTITY updateEntity(@NotNull ENTITY e) throws UniqueException;

    void deleteEntity(@NotNull ENTITY e) throws E;

    @NotNull
    default InsertEntityEvent<PK, ENTITY> insertEntityEvent(@NotNull ENTITY entity) {
        return new InsertEntityEvent<>(this, entity);
    }

    @NotNull
    default UpdateEntityEvent<PK, ENTITY> updateEntityEvent(@NotNull ENTITY before, @NotNull ENTITY after) {
        return new UpdateEntityEvent<>(this, before, after);
    }

    @NotNull
    default DeleteEntityEvent<PK, ENTITY> deleteEntityEvent(@NotNull ENTITY entity) {
        return new DeleteEntityEvent<>(this, entity);
    }

}
