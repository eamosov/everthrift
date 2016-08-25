package org.everthrift.appserver.model.events;

import org.everthrift.appserver.model.DaoEntityIF;
import org.everthrift.appserver.model.RoModelFactoryIF;

public class InsertEntityEvent<PK, ENTITY extends DaoEntityIF> {

    public final RoModelFactoryIF<PK, ENTITY> factory;

    public final ENTITY entity;

    public InsertEntityEvent(RoModelFactoryIF<PK, ENTITY> factory, ENTITY entity) {
        super();
        this.factory = factory;
        this.entity = entity;
    }
}
