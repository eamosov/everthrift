package com.knockchat.node.model.events;

import com.knockchat.hibernate.dao.DaoEntityIF;
import com.knockchat.node.model.RoModelFactoryIF;

public class DeleteEntityEvent<PK, ENTITY extends DaoEntityIF> {

	public final RoModelFactoryIF<PK, ENTITY> factory;
	public final ENTITY entity;
	
	public DeleteEntityEvent(RoModelFactoryIF<PK, ENTITY> factory, ENTITY entity) {
		super();
		this.factory = factory;
		this.entity = entity;
	}	
}
