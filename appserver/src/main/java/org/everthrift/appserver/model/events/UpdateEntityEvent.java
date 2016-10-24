package org.everthrift.appserver.model.events;

import org.everthrift.appserver.model.DaoEntityIF;
import org.everthrift.appserver.model.RoModelFactoryIF;

import java.io.Serializable;

public class UpdateEntityEvent<PK extends Serializable, ENTITY extends DaoEntityIF> {

    public final RoModelFactoryIF<PK, ENTITY> factory;

    public final ENTITY beforeUpdate;

    public final ENTITY afterUpdate;

    public final PK updatedByPk;

    public UpdateEntityEvent(RoModelFactoryIF<PK, ENTITY> factory, ENTITY beforeUpdate, ENTITY afterUpdate) {
        super();
        this.factory = factory;
        this.beforeUpdate = beforeUpdate;
        this.afterUpdate = afterUpdate;
        this.updatedByPk = null;
    }

    public UpdateEntityEvent(RoModelFactoryIF<PK, ENTITY> factory, PK updatedByPk) {
        super();
        this.factory = factory;
        this.beforeUpdate = null;
        this.afterUpdate = null;
        this.updatedByPk = updatedByPk;
    }

    @Override
    public String toString() {
        return "UpdateEntityEvent{" +
            "factory=" + factory +
            ", beforeUpdate=" + beforeUpdate +
            ", afterUpdate=" + afterUpdate +
            ", updatedByPk=" + updatedByPk +
            '}';
    }
}
