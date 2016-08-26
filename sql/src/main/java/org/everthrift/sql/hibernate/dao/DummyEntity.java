package org.everthrift.sql.hibernate.dao;

import org.everthrift.appserver.model.DaoEntityIF;

import java.io.Serializable;

public class DummyEntity implements DaoEntityIF {

    public DummyEntity() {
    }

    @Override
    public Serializable getPk() {
        return null;
    }

    @Override
    public void setPk(Serializable identifier) {
    }

}
