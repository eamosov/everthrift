package org.everthrift.sql.hibernate.dao;

import org.everthrift.appserver.model.DaoEntityIF;
import org.jetbrains.annotations.Nullable;

import java.io.Serializable;

public class DummyEntity implements DaoEntityIF {

    public DummyEntity() {
    }

    @Nullable
    @Override
    public Serializable getPk() {
        return null;
    }

    @Override
    public void setPk(Serializable identifier) {
    }

}
