package com.knockchat.appserver.model.events;

import com.knockchat.appserver.model.RoModelFactoryIF;
import com.knockchat.hibernate.dao.DaoEntityIF;

public class DeleteEntityEvent<PK, ENTITY extends DaoEntityIF> {

	public final RoModelFactoryIF<PK, ENTITY> factory;
	public final ENTITY entity;
	
	public DeleteEntityEvent(RoModelFactoryIF<PK, ENTITY> factory, ENTITY entity) {
		super();
		this.factory = factory;
		this.entity = entity;
	}	
}
