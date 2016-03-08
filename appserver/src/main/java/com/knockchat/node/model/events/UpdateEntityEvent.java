package com.knockchat.node.model.events;

import com.knockchat.hibernate.dao.DaoEntityIF;
import com.knockchat.node.model.RoModelFactoryIF;

public class UpdateEntityEvent<PK, ENTITY extends DaoEntityIF> {
	
	public final RoModelFactoryIF<PK, ENTITY> factory;
	public final ENTITY beforeUpdate;
	public final ENTITY afterUpdate;
	
	public UpdateEntityEvent(RoModelFactoryIF<PK, ENTITY> factory, ENTITY beforeUpdate, ENTITY afterUpdate) {
		super();
		this.factory = factory;
		this.beforeUpdate = beforeUpdate;
		this.afterUpdate = afterUpdate;
	}
		
	@Override
	public String toString() {
		return this.getClass().getCanonicalName() + " [factory=" + factory + ", beforeUpdate=" + beforeUpdate + ", afterUpdate="
				+ afterUpdate + "]";
	}
	
}
