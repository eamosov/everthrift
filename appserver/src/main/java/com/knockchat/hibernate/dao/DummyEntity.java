package com.knockchat.hibernate.dao;

import java.io.Serializable;

public class DummyEntity implements DaoEntityIF<DummyEntity> {

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
