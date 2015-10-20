package com.knockchat.node.model.pgsql;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.Callable;

import org.apache.thrift.TException;
import org.hibernate.StaleStateException;
import org.hibernate.exception.ConstraintViolationException;
import org.springframework.dao.ConcurrencyFailureException;
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
import com.knockchat.utils.thrift.TFunction;

public abstract class OptimisticLockPgSqlModelFactory<PK extends Serializable,ENTITY extends DaoEntityIF, E extends TException> extends AbstractPgSqlModelFactory<PK, ENTITY> implements OptimisticLockModelFactoryIF<PK, ENTITY, E>  {
	
	protected abstract E createNotFoundException(PK id);
	
	/**
	 * Cache need only because Hibernate does not cache rows selected by "IN" statement
	 * @param cacheName
	 * @param entityClass
	 */
    public OptimisticLockPgSqlModelFactory(String cacheName, Class<ENTITY> entityClass) {
        super(cacheName, entityClass);
    }
    
    @Override
	public OptimisticUpdateResult<ENTITY> update(final PK id, final TFunction<ENTITY, Boolean> mutator, final EntityFactory<PK, ENTITY> factory) throws TException, E{    	
    	
    	return update(id, new EntityMutator<ENTITY>(){

			@Override
			public boolean beforeUpdate() throws TException {
				return true;
			}

			@Override
			public boolean update(ENTITY e) throws TException {
				return mutator.apply(e);
			}

			@Override
			public void afterTransactionClosed() {
			}

			@Override
			public void afterUpdate() {
			}}, factory);
    }

    @Override
	public OptimisticUpdateResult<ENTITY> update(PK id, EntityMutator<ENTITY> mutator, final EntityFactory<PK, ENTITY> factory) throws TException, E{
		try {
			return  this.optimisticUpdate(id, mutator, factory);
		} catch (EntityNotFoundException e) {
			throw createNotFoundException(id);
		}
	}
    
    @Transactional
    private ENTITY tryOptimisticDelete(PK id) throws EntityNotFoundException{
    	
    	ENTITY e = this.fetchEntityById(id);
    	if (e == null)
    		throw new EntityNotFoundException(id);
    	
    	this.getDao().delete(e);
    	return e;
    }
    
    public void deleteEntity(ENTITY e){
    	throw new UnsupportedOperationException();
    }
    
    public ENTITY delete(final PK id) throws E {
    	
    	if (TransactionSynchronizationManager.isActualTransactionActive())
    		throw new RuntimeException("Can't correctly do optimistic update within transaction");

    	try {
    		return RwModelFactoryHelper.optimisticUpdate( () ->
				{
					try{						
						return tryOptimisticDelete(id); 
					}catch(StaleStateException | ConcurrencyFailureException e){
						if (TransactionSynchronizationManager.isActualTransactionActive()) {
							throw e;
						}
						log.debug("update fails id={}, let's try one more time? {}", id, e.getMessage());
						return null;
					}
				},
				RwModelFactoryHelper.MAX_ITERATIONS, RwModelFactoryHelper.MAX_TIMEOUT);
		} catch (EntityNotFoundException e) {
			throw createNotFoundException(id); 
		} catch (TException e) {
			throw Throwables.propagate(e);
		}finally{
			_invalidate(id);			
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
    private OptimisticUpdateResult<ENTITY> optimisticUpdate(final PK id, final EntityMutator<ENTITY> mutator, final EntityFactory<PK, ENTITY> factory) throws TException, EntityNotFoundException, StaleStateException, ConcurrencyFailureException {
    	
    	if (TransactionSynchronizationManager.isActualTransactionActive())
    		throw new RuntimeException("Can't correctly do optimistic update within transaction");
    			
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
					}catch(StaleStateException | ConcurrencyFailureException e ){
						if (TransactionSynchronizationManager.isActualTransactionActive()) {
							throw e;
						}
						log.debug("update fails id={}, let's try one more time? {}", id, e.getMessage());
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
	private OptimisticUpdateResult<ENTITY> tryOptimisticUpdate(PK id, EntityMutator<ENTITY> mutator, final EntityFactory<PK, ENTITY> factory) throws TException, EntityNotFoundException, StaleStateException, ConcurrencyFailureException{
		
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
		}catch (EntityNotFoundException e){
			log.debug("tryOptimisticUpdate ends with exception of type {}", e.getClass().getSimpleName());
			throw e;
		}catch(TException e){
			log.warn("tryOptimisticUpdate ends with exception of type {}", e.getClass().getSimpleName());
			throw e;
		}catch (StaleStateException | ConcurrencyFailureException e){
			throw e;
		}catch (Exception e){		
			log.warn("tryOptimisticUpdate ends with exception of type {}", e.getClass().getSimpleName());
			throw Throwables.propagate(e);
		}				
	}
     
}

