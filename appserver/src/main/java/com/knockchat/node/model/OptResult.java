package com.knockchat.node.model;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.commons.collections.CollectionUtils;

import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
import com.knockchat.hibernate.dao.DaoEntityIF;

public class OptResult<ENTITY extends DaoEntityIF> {
	
	public static final OptResult CANCELED =  OptResult.create(null, null, null, false);
	
	public final OptimisticLockModelFactoryIF<?, ENTITY, ?> factory;
	public final ENTITY afterUpdate;
	public final ENTITY beforeUpdate;
	public final boolean isUpdated; // true, если произошло изменение объекта в БД
	
	public List<OptResult> inner;
	
	public OptResult(OptimisticLockModelFactoryIF<?, ENTITY, ?> factory, ENTITY afterUpdate, ENTITY beforeUpdate, boolean isUpdated) {
		super();
		this.afterUpdate = afterUpdate;
		this.beforeUpdate = beforeUpdate;
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
		return isUpdated && beforeUpdate ==null;
	}
	
	public <T extends DaoEntityIF> void add(OptResult<T> e){
		if (inner == null)
			inner = Lists.newArrayList();
		
		inner.add(e);
	}
	
	public <PK, T extends DaoEntityIF> Collection<OptResult> getInner(OptimisticLockModelFactoryIF<PK, T, ?> factory){
		if (CollectionUtils.isEmpty(inner))
			return Collections.emptyList();
		
		return Collections2.filter(inner, r ->(r.factory == factory));
	}
	
	public <PK, T extends DaoEntityIF> T getInnerUpdated(OptimisticLockModelFactoryIF<PK, T, ?> factory, T defaultValue){
		
		if (CollectionUtils.isEmpty(inner))
			return defaultValue;
		
		for (OptResult r: inner){
			if (r.factory == factory && r.afterUpdate !=null && r.afterUpdate.getPk().equals(defaultValue.getPk()))
				return (T)r.afterUpdate;
		}
		return defaultValue;
	}

	@Override
	public String toString() {
		return "OptResult [factory=" + factory + ", afterUpdate=" + afterUpdate + ", beforeUpdate=" + beforeUpdate
				+ ", isUpdated=" + isUpdated + ", inner=" + inner + "]";
	}	
}