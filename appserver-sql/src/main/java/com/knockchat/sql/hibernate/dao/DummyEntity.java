package com.knockchat.sql.hibernate.dao;

import java.io.Serializable;

import com.knockchat.appserver.model.DaoEntityIF;

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
