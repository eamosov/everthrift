package com.knockchat.node.model;

import org.apache.thrift.TException;

import com.knockchat.hibernate.dao.DaoEntityIF;
import com.knockchat.utils.thrift.TFunction;

public interface OptimisticLockModelFactoryIF<PK, ENTITY extends DaoEntityIF, E extends TException> extends RwModelFactoryIF<PK, ENTITY> {
	
	OptResult<ENTITY> updateUnchecked(PK id, TFunction<ENTITY, Boolean> mutator);
    OptResult<ENTITY> updateUnchecked(PK id, TFunction<ENTITY, Boolean> mutator, final EntityFactory<PK, ENTITY> factory);
	
	OptResult<ENTITY> update(PK id, TFunction<ENTITY, Boolean> mutator) throws TException, E;
	OptResult<ENTITY> update(PK id, TFunction<ENTITY, Boolean> mutator, final EntityFactory<PK, ENTITY> factory) throws TException, E;
	
	OptResult<ENTITY> delete(final PK id) throws E;
	OptResult<ENTITY> optInsert(final ENTITY e);		
}
