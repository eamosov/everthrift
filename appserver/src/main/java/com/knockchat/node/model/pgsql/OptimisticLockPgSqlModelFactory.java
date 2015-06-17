package com.knockchat.node.model.pgsql;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.Callable;

import org.apache.thrift.TException;
import org.hibernate.StaleObjectStateException;
import org.hibernate.exception.ConstraintViolationException;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import com.google.common.base.Throwables;
import com.knockchat.hibernate.dao.DaoEntityIF;
import com.knockchat.node.model.EntityFactory;
import com.knockchat.node.model.EntityMutator;
import com.knockchat.node.model.EntityNotFoundException;
import com.knockchat.node.model.OptimisticLockModelFactoryIF;
import com.knockchat.node.model.OptimisticUpdateResult;
import com.knockchat.node.model.RwModelFactoryHelper;

public abstract class OptimisticLockPgSqlModelFactory<PK extends Serializable,ENTITY extends DaoEntityIF, A, E extends TException> extends AbstractPgSqlModelFactory<PK, ENTITY, A> implements OptimisticLockModelFactoryIF<PK, ENTITY, E>  {
	
	protected abstract E createNotFoundException(PK id);
	
    public OptimisticLockPgSqlModelFactory(String cacheName, Class<ENTITY> entityClass) {
        super(cacheName, entityClass);
    }    

    @Override
	public OptimisticUpdateResult<ENTITY> update(PK id, EntityMutator<ENTITY> mutator, final EntityFactory<PK, ENTITY> factory) throws TException, E{
		try {
			return  this.optimisticUpdate(id, mutator, factory);
		} catch (EntityNotFoundException e) {
			throw createNotFoundException(id);
		}
	}

	@Override
	@Transactional
    public ENTITY fetchEntityById(PK id){
    	return super.fetchEntityById(id);
    }

	@Override
	@Transactional	
    public Map<PK,ENTITY> fetchEntityByIdAsMap(Collection<PK> ids){
    	return super.fetchEntityByIdAsMap(ids);
    }
            
	/**
	 * 
	 * @param id
	 * @param mutator
	 * @param factory
	 * @return <new, old>
	 * @throws Exception
	 */
    private OptimisticUpdateResult<ENTITY> optimisticUpdate(final PK id, final EntityMutator<ENTITY> mutator, final EntityFactory<PK, ENTITY> factory) throws TException, EntityNotFoundException {
		
    	final OptimisticUpdateResult<ENTITY> ret =  RwModelFactoryHelper.optimisticUpdate(new Callable<OptimisticUpdateResult<ENTITY>>(){

				@Override
				public OptimisticUpdateResult<ENTITY> call() throws Exception {
					try{
						if (mutator.beforeUpdate()){
							final OptimisticUpdateResult<ENTITY> ret = tryOptimisticUpdate(id, mutator, factory);
							if (ret.isUpdated){
								mutator.afterUpdate();
							}
							return ret;
						}else{
							return OptimisticUpdateResult.CANCELED;
						}
					}catch (ConstraintViolationException e){
						if (e.getConstraintName().contains("pkey"))
							return null;
						else
							throw Throwables.propagate(e);
					}catch(StaleObjectStateException e){
						if (TransactionSynchronizationManager.isActualTransactionActive()) {
							throw e;
						}
						log.debug("update fails id= " + id + ", let's try one more time?", e);
						return null;
					}finally{
						mutator.afterTransactionClosed();
					}
				}}, RwModelFactoryHelper.MAX_ITERATIONS, RwModelFactoryHelper.MAX_TIMEOUT);
			
			if (ret.isUpdated)
				_invalidate(id);

			return ret;
	}
	
	@Transactional(rollbackFor=Exception.class)
	private OptimisticUpdateResult<ENTITY> tryOptimisticUpdate(PK id, EntityMutator<ENTITY> mutator, final EntityFactory<PK, ENTITY> factory) throws TException, EntityNotFoundException{
		
		try{
			ENTITY e;
			final ENTITY orig;
			
			e = this.fetchEntityById(id);
			if (e == null){
				if (factory == null)
					throw new EntityNotFoundException(id);
			
				orig = null;
				e = factory.create(id);
				getDao().persist(e);
			}else{
				try {
					orig = this.entityClass.getConstructor(this.entityClass).newInstance(e);
				} catch (InstantiationException | IllegalAccessException
						| IllegalArgumentException | InvocationTargetException
						| NoSuchMethodException | SecurityException e1) {
					throw Throwables.propagate(e1);
				}			
			}
			
			
			if (mutator.update(e)){
				return OptimisticUpdateResult.create(helper.updateEntity(e), orig, helper.isUpdated());
			}else{
				return OptimisticUpdateResult.create(e, e, false);
			}			
		}catch(TException | EntityNotFoundException e){
			log.warn("tryOptimisticUpdate ends with exception of type {}", e.getClass().getSimpleName());
			throw e;
		}catch (Exception e){		
			log.warn("tryOptimisticUpdate ends with exception of type {}", e.getClass().getSimpleName());
			throw Throwables.propagate(e);
		}				
	}
     
}

