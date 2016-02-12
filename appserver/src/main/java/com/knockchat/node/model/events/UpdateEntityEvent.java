package com.knockchat.node.model.events;

import com.knockchat.hibernate.dao.DaoEntityIF;
import com.knockchat.node.model.OptResult;
import com.knockchat.node.model.RoModelFactoryIF;

public abstract class UpdateEntityEvent<PK, ENTITY extends DaoEntityIF> {
	
	public final RoModelFactoryIF<PK, ENTITY> factory;
	public final ENTITY beforeUpdate;
	public final ENTITY afterUpdate;
	
	UpdateEntityEvent(RoModelFactoryIF<PK, ENTITY> factory, ENTITY beforeUpdate, ENTITY afterUpdate) {
		super();
		this.factory = factory;
		this.beforeUpdate = beforeUpdate;
		this.afterUpdate = afterUpdate;
	}
	
	

	UpdateEntityEvent(RoModelFactoryIF<PK, ENTITY> factory, OptResult<ENTITY> optResult) {
		super();
		this.factory = factory;
		this.beforeUpdate = optResult.beforeUpdate;
		this.afterUpdate = optResult.afterUpdate;
	}

	public static class AsyncUpdateEntityEvent<PK, ENTITY extends DaoEntityIF> extends UpdateEntityEvent<PK, ENTITY>{

		public AsyncUpdateEntityEvent(RoModelFactoryIF<PK, ENTITY> factory, ENTITY beforeUpdate, ENTITY afterUpdate) {
			super(factory, beforeUpdate, afterUpdate);
		}		
	}
	
	public static class SyncUpdateEntityEvent<PK, ENTITY extends DaoEntityIF> extends UpdateEntityEvent<PK, ENTITY>{

		public final OptResult<ENTITY> optResult;

		public SyncUpdateEntityEvent(RoModelFactoryIF<PK, ENTITY> factory, OptResult<ENTITY> optResult) {
			super(factory, optResult.beforeUpdate, optResult.afterUpdate);
			this.optResult = optResult;
		}
		
		public SyncUpdateEntityEvent(RoModelFactoryIF<PK, ENTITY> factory, ENTITY beforeUpdate, ENTITY afterUpdate) {
			super(factory, beforeUpdate, afterUpdate);
			this.optResult = null;
		}		
	}

	@Override
	public String toString() {
		return this.getClass().getCanonicalName() + " [factory=" + factory + ", beforeUpdate=" + beforeUpdate + ", afterUpdate="
				+ afterUpdate + "]";
	}
	
}
