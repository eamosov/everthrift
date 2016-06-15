package org.everthrift.appserver.model.events;

import org.everthrift.appserver.model.DaoEntityIF;
import org.everthrift.appserver.model.RoModelFactoryIF;

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
