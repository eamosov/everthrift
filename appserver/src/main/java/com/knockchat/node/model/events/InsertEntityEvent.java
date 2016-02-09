package com.knockchat.node.model.events;

import com.knockchat.hibernate.dao.DaoEntityIF;
import com.knockchat.node.model.OptResult;
import com.knockchat.node.model.RoModelFactoryIF;

public abstract class InsertEntityEvent<PK, ENTITY extends DaoEntityIF> {

	public final RoModelFactoryIF<PK, ENTITY> factory;
	public final ENTITY entity;
	
	InsertEntityEvent(RoModelFactoryIF<PK, ENTITY> factory, ENTITY entity) {
		super();
		this.factory = factory;
		this.entity = entity;
	}

	public static class AsyncInsertEntityEvent<PK, ENTITY extends DaoEntityIF> extends InsertEntityEvent<PK, ENTITY>{
		
		public AsyncInsertEntityEvent(RoModelFactoryIF<PK, ENTITY> factory, ENTITY entity) {
			super(factory, entity);
		}
	}

	public static class SyncInsertEntityEvent<PK, ENTITY extends DaoEntityIF> extends InsertEntityEvent<PK, ENTITY>{
		
		public final OptResult<ENTITY> optResult;
		
		public SyncInsertEntityEvent(RoModelFactoryIF<PK, ENTITY> factory, ENTITY entity) {
			super(factory, entity);
			this.optResult = null;
		}
		
		public SyncInsertEntityEvent(RoModelFactoryIF<PK, ENTITY> factory, OptResult<ENTITY> optResult) {
			super(factory, optResult.updated);
			this.optResult = optResult;
		}
		
	}

}
