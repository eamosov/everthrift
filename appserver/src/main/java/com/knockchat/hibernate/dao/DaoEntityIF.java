package com.knockchat.hibernate.dao;

import java.io.Serializable;

public interface DaoEntityIF<V> extends Serializable  {

    public Serializable getPk();

    public void setPk(Serializable identifier);    

}
