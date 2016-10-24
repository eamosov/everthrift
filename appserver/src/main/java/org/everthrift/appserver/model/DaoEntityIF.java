package org.everthrift.appserver.model;

import java.io.Serializable;

public interface DaoEntityIF extends Serializable {

    Serializable getPk();

    void setPk(Serializable identifier);

}
