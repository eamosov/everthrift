package com.knockchat.node.model;

import org.apache.thrift.TException;

public interface OptimisticLockModelFactoryIF<PK, ENTITY, E extends TException> extends RwModelFactoryIF<PK, ENTITY> {
		
	OptimisticUpdateResult<ENTITY> update(PK id, EntityMutator<ENTITY> mutator, final EntityFactory<PK, ENTITY> factory) throws TException, E;
	
	
	
	/** 
	 * same as previous, but with functional interface
	 * */
	@FunctionalInterface
	public static interface LambdaMutator<ENTITY>{
		boolean apply(ENTITY e) throws TException;
	}
	
	OptimisticUpdateResult<ENTITY> update(PK id, LambdaMutator<ENTITY> mutator, final EntityFactory<PK, ENTITY> factory) throws TException, E;
}
