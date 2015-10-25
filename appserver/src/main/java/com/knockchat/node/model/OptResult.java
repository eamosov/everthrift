package com.knockchat.node.model;

import java.util.List;

import org.apache.commons.collections.CollectionUtils;

import com.google.common.collect.Lists;
import com.knockchat.hibernate.dao.DaoEntityIF;

public class OptResult<ENTITY extends DaoEntityIF> {
	
	public static final OptResult CANCELED =  OptResult.create(null, null, null, false);
	
	public final OptimisticLockModelFactoryIF<?, ENTITY, ?> factory;
	public final ENTITY updated;
	public final ENTITY old;
	public final boolean isUpdated; // true, если произошло изменение объекта в БД
	
	public List<OptResult> inner;
	
	public OptResult(OptimisticLockModelFactoryIF<?, ENTITY, ?> factory, ENTITY updated, ENTITY old, boolean isUpdated) {
		super();
		this.updated = updated;
		this.old = old;
		this.isUpdated = isUpdated;
		this.factory = factory;
	}
	
	public static <ENTITY extends DaoEntityIF> OptResult<ENTITY> create(OptimisticLockModelFactoryIF<?, ENTITY, ?> factory, ENTITY updated, ENTITY old, boolean isUpdated){
		return new OptResult<ENTITY>(factory, updated, old, isUpdated);
	}
	
	public boolean isCanceled(){
		return this == CANCELED;
	}
	
	public boolean isInserted(){
		return isUpdated && old ==null;
	}
	
	public <T extends DaoEntityIF> void add(OptResult<T> e){
		if (inner == null)
			inner = Lists.newArrayList();
		
		inner.add(e);
	}
	
	public <PK, T extends DaoEntityIF> T getInnerUpdated(OptimisticLockModelFactoryIF<PK, T, ?> factory, T defaultValue){
		
		if (CollectionUtils.isEmpty(inner))
			return defaultValue;
		
		for (OptResult r: inner){
			if (r.factory == factory && r.updated !=null && r.updated.getPk().equals(defaultValue.getPk()))
				return (T)r.updated;
		}
		return defaultValue;
	}
}