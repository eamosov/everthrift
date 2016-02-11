package com.knockchat.node.model.events;

import com.knockchat.hibernate.dao.DaoEntityIF;
import com.knockchat.node.model.OptResult;
import com.knockchat.node.model.RoModelFactoryIF;

public abstract class DeleteEntityEvent<PK, ENTITY extends DaoEntityIF> {

	public final RoModelFactoryIF<PK, ENTITY> factory;
	public final ENTITY entity;
	
	DeleteEntityEvent(RoModelFactoryIF<PK, ENTITY> factory, ENTITY entity) {
		super();
		this.factory = factory;
		this.entity = entity;
	}

	public static class AsyncDeleteEntityEvent<PK, ENTITY extends DaoEntityIF> extends DeleteEntityEvent<PK, ENTITY>{
		public AsyncDeleteEntityEvent(RoModelFactoryIF<PK, ENTITY> factory, ENTITY entity) {
			super(factory, entity);
		}		
	}
	
	public static class SyncDeleteEntityEvent<PK, ENTITY extends DaoEntityIF> extends DeleteEntityEvent<PK, ENTITY>{
		
		public final OptResult<ENTITY> optResult;
		
		public SyncDeleteEntityEvent(RoModelFactoryIF<PK, ENTITY> factory, ENTITY entity) {
			super(factory, entity);
			this.optResult = null;
		}
		
		public SyncDeleteEntityEvent(RoModelFactoryIF<PK, ENTITY> factory, OptResult<ENTITY> optResult) {
			super(factory, optResult.beforeUpdate);
			this.optResult = optResult;
		}				
	}
	
}
