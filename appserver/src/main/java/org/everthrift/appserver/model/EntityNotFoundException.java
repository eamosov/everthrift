package org.everthrift.appserver.model;

public class EntityNotFoundException extends Exception{

    private static final long serialVersionUID = 1L;

    public final Object id;

    public EntityNotFoundException(Object id){
        this.id = id;
    }
}
