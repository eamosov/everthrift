package com.knockchat.node.model;


public class OptimisticUpdateResult<ENTITY> {
	
	public static final OptimisticUpdateResult CANCELED =  OptimisticUpdateResult.create(null, null, false);
	
	public final ENTITY updated;
	public final ENTITY old;
	public final boolean isUpdated; // true, если произошло изменение объекта в БД
	
	public OptimisticUpdateResult(ENTITY updated, ENTITY old, boolean isUpdated) {
		super();
		this.updated = updated;
		this.old = old;
		this.isUpdated = isUpdated;
	}
	
	public static <ENTITY> OptimisticUpdateResult<ENTITY> create(ENTITY updated, ENTITY old, boolean isUpdated){
		return new OptimisticUpdateResult<ENTITY>(updated, old, isUpdated);
	}
	
	public boolean isCanceled(){
		return this == CANCELED;
	}
	
	public boolean isInserted(){
		return isUpdated && old ==null;
	}
}