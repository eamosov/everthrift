package org.everthrift.appserver.model.events;

import org.everthrift.appserver.model.DaoEntityIF;
import org.everthrift.appserver.model.RoModelFactoryIF;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.Serializable;

public class UpdateEntityEvent<PK extends Serializable, ENTITY extends DaoEntityIF> {

    public final RoModelFactoryIF<PK, ENTITY, ?> factory;

    @NotNull
    public final ENTITY beforeUpdate;

    @NotNull
    public final ENTITY afterUpdate;

    @Nullable
    //TODO что это??
    public final PK updatedByPk;

    public UpdateEntityEvent(RoModelFactoryIF<PK, ENTITY, ?> factory, @NotNull ENTITY beforeUpdate, @NotNull ENTITY afterUpdate) {
        super();
        this.factory = factory;
        this.beforeUpdate = beforeUpdate;
        this.afterUpdate = afterUpdate;
        this.updatedByPk = null;
    }

//    public UpdateEntityEvent(RoModelFactoryIF<PK, ENTITY, ?> factory, PK updatedByPk) {
//        super();
//        this.factory = factory;
//        this.beforeUpdate = null;
//        this.afterUpdate = null;
//        this.updatedByPk = updatedByPk;
//    }

    @NotNull
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
