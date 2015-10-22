package com.knockchat.node.model;

import org.apache.thrift.TException;

import com.knockchat.utils.thrift.TFunction;

public interface OptimisticLockModelFactoryIF<PK, ENTITY, E extends TException> extends RwModelFactoryIF<PK, ENTITY> {
				
	OptimisticUpdateResult<ENTITY> update(PK id, TFunction<ENTITY, Boolean> mutator, final EntityFactory<PK, ENTITY> factory) throws TException, E;
}
