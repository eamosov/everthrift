package com.knockchat.appserver.model.events;

import com.knockchat.appserver.model.DaoEntityIF;
import com.knockchat.appserver.model.RoModelFactoryIF;

public class InsertEntityEvent<PK, ENTITY extends DaoEntityIF> {

	public final RoModelFactoryIF<PK, ENTITY> factory;
	public final ENTITY entity;
	
	public InsertEntityEvent(RoModelFactoryIF<PK, ENTITY> factory, ENTITY entity) {
		super();
		this.factory = factory;
		this.entity = entity;
	}
}
