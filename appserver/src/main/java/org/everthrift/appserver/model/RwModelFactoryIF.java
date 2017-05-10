package org.everthrift.appserver.model;

import org.apache.thrift.TException;
import org.everthrift.appserver.model.events.DeleteEntityEvent;
import org.everthrift.appserver.model.events.InsertEntityEvent;
import org.everthrift.appserver.model.events.UpdateEntityEvent;

import java.io.Serializable;

public interface RwModelFactoryIF<PK extends Serializable, ENTITY extends DaoEntityIF, E extends TException> extends RoModelFactoryIF<PK, ENTITY, E> {

    ENTITY insertEntity(ENTITY e) throws UniqueException;

    default ENTITY updateEntity(ENTITY e) throws UniqueException {
        return updateEntity(e, null);
    }

    ENTITY updateEntity(ENTITY e, ENTITY old) throws UniqueException;

    void deleteEntity(ENTITY e) throws E;

    default InsertEntityEvent<PK, ENTITY> insertEntityEvent(ENTITY entity) {
        return new InsertEntityEvent<>(this, entity);
    }

    default UpdateEntityEvent<PK, ENTITY> updateEntityEvent(ENTITY before, ENTITY after) {
        return new UpdateEntityEvent<>(this, before, after);
    }

    default DeleteEntityEvent<PK, ENTITY> deleteEntityEvent(ENTITY entity) {
        return new DeleteEntityEvent<>(this, entity);
    }

}
