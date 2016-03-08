package com.knockchat.node.model.pgsql;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import org.apache.thrift.TException;
import org.hibernate.SessionFactory;
import org.hibernate.StaleStateException;
import org.hibernate.criterion.Restrictions;
import org.springframework.dao.ConcurrencyFailureException;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.knockchat.appserver.model.CreatedAtIF;
import com.knockchat.appserver.model.UpdatedAtIF;
import com.knockchat.hibernate.dao.DaoEntityIF;
import com.knockchat.node.model.EntityFactory;
import com.knockchat.node.model.EntityNotFoundException;
import com.knockchat.node.model.LocalEventBus;
import com.knockchat.node.model.OptResult;
import com.knockchat.node.model.OptimisticLockModelFactoryIF;
import com.knockchat.node.model.UniqueException;
import com.knockchat.utils.LongTimestamp;
import com.knockchat.utils.Pair;
import com.knockchat.utils.thrift.TFunction;

import net.sf.ehcache.Cache;

public abstract class OptimisticLockPgSqlModelFactory<PK extends Serializable,ENTITY extends DaoEntityIF, E extends TException> extends AbstractPgSqlModelFactory<PK, ENTITY, E> implements OptimisticLockModelFactoryIF<PK, ENTITY, E>  {
	
	protected abstract E createNotFoundException(PK id);
	
	/**
	 * Cache need only because Hibernate does not cache rows selected by "IN" statement
	 * @param cacheName
	 * @param entityClass
	 */
    public OptimisticLockPgSqlModelFactory(String cacheName, Class<ENTITY> entityClass) {
        super(cacheName, entityClass);
    }
    
    
    
    public OptimisticLockPgSqlModelFactory(Cache cache, Class<ENTITY> entityClass, ListeningExecutorService listeningExecutorService, SessionFactory sessionFactory, LocalEventBus localEventBus) {
		super(cache, entityClass, listeningExecutorService, sessionFactory, localEventBus);
	}

	@Override
    public final OptResult<ENTITY> updateUnchecked(PK id, TFunction<ENTITY, Boolean> mutator){
    	try {
    		return update(id, mutator);
		} catch (Exception e) {
			throw Throwables.propagate(e);
		}    	
    }

    
    @Override
	public final OptResult<ENTITY> updateUnchecked(PK id, TFunction<ENTITY, Boolean> mutator, final EntityFactory<PK, ENTITY> factory){
		try {
			return  update(id, mutator, factory);
		} catch (Exception e) {
			throw Throwables.propagate(e);
		}
	}
    
    @Override
    public final OptResult<ENTITY> update(PK id, TFunction<ENTITY, Boolean> mutator) throws TException, E{
    	return update(id, mutator, null);
    }


    @Override
	public final OptResult<ENTITY> update(PK id, TFunction<ENTITY, Boolean> mutator, final EntityFactory<PK, ENTITY> factory) throws TException, E{
		try {
			return optimisticUpdate(id, mutator, factory);			
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
        
    @Override
    public final OptResult<ENTITY> delete(final PK id) throws E {
    	
    	if (TransactionSynchronizationManager.isActualTransactionActive())
    		throw new RuntimeException("Can't correctly do optimistic update within transaction");

    	try {
    		final ENTITY deleted = OptimisticLockModelFactoryIF.optimisticUpdate( (count) ->
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
				});

    		final OptResult<ENTITY> r = OptResult.create(this, null, deleted, true); 
        	localEventBus.postAsync(deleteEntityEvent(deleted));    		
    		return r;
		} catch (EntityNotFoundException e) {
			throw createNotFoundException(id); 
		} catch (TException e) {
			throw Throwables.propagate(e);
		}finally{
			_invalidateEhCache(id);			
		}
    }
    
	@Override
	public final ENTITY insertEntity(ENTITY e) {
		try {
			return this.optInsert(e).afterUpdate;
		} catch (Exception e1) {
			throw Throwables.propagate(e1);
		}
	}
    
    @Override
    public final OptResult<ENTITY> optInsert(final ENTITY e){
    	
    	final long now = LongTimestamp.now();
    	
    	if (e instanceof CreatedAtIF){
    		((CreatedAtIF)e).setCreatedAt(now);
    	}

    	if (e instanceof UpdatedAtIF){
    		((UpdatedAtIF)e).setUpdatedAt(now);
    	}

		final ENTITY inserted = getDao().save(e).first;
		_invalidateEhCache((PK)inserted.getPk());
		
		final OptResult<ENTITY> r = OptResult.create(this, inserted, null, true); 
    	localEventBus.postAsync(insertEntityEvent(inserted));    	
    	return r;
    }

	@Override
	@Transactional
    protected final ENTITY fetchEntityById(PK id){
    	return super.fetchEntityById(id);
    }

	@Override
	@Transactional	
	protected final Map<PK,ENTITY> fetchEntityByIdAsMap(Collection<PK> ids){
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
    private OptResult<ENTITY> optimisticUpdate(final PK id, final TFunction<ENTITY, Boolean> mutator, final EntityFactory<PK, ENTITY> factory) throws TException, EntityNotFoundException, StaleStateException, ConcurrencyFailureException {
    	
    	if (TransactionSynchronizationManager.isActualTransactionActive())
    		throw new RuntimeException("Can't correctly do optimistic update within transaction");
    			
    	final OptResult<ENTITY> ret =  OptimisticLockModelFactoryIF.optimisticUpdate((count) -> {
					try{
						return tryOptimisticUpdate(id, mutator, factory);
					}catch (UniqueException e){
						if (e.isPrimaryKey())
							return null;
						else
							throw Throwables.propagate(e);
					}catch(StaleStateException | ConcurrencyFailureException e ){
						if (TransactionSynchronizationManager.isActualTransactionActive()) {
							throw e;
						}
						log.debug("update fails id={}, let's try one more time? {}", id, e.getMessage());
						return null;
					}
				});
		
		if (ret.isUpdated){
			_invalidateEhCache(id);
			
	    	localEventBus.postAsync(updateEntityEvent(ret.beforeUpdate, ret.afterUpdate));			
		}

		return ret;
	}
	
	@Transactional(rollbackFor=Exception.class)
	private OptResult<ENTITY> tryOptimisticUpdate(PK id, TFunction<ENTITY, Boolean> mutator, final EntityFactory<PK, ENTITY> factory) throws TException, EntityNotFoundException, StaleStateException, ConcurrencyFailureException{
		
		try{
			ENTITY e;
			final ENTITY orig;
			
			e = this.fetchEntityById(id);
			if (e == null){
				if (factory == null)
					throw new EntityNotFoundException(id);
			
				orig = null;
				e = factory.create(id);
				
		    	final long now = LongTimestamp.now();
		    	
		    	if (e instanceof CreatedAtIF){
		    		((CreatedAtIF)e).setCreatedAt(now);
		    	}

		    	if (e instanceof UpdatedAtIF){
		    		((UpdatedAtIF)e).setUpdatedAt(now);
		    	}
				
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
			
			
			if (mutator.apply(e)){				
				final Pair<ENTITY, Boolean> r = getDao().saveOrUpdate(e);
				return OptResult.create(this, r.first, orig, r.second);
			}else{
				return OptResult.create(this, e, e, false);
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

	@Override
	@Transactional
	public final ENTITY updateEntity(ENTITY e) throws UniqueException {
		
		final ENTITY before;
		if (e.getPk()!=null){
			before = getDao().findById((PK)e.getPk());
		}else{
			before = null;
		}

		final Pair<ENTITY, Boolean> r = getDao().saveOrUpdate(e);
		
		final OptResult<ENTITY> ret = new OptResult<ENTITY>(this, r.first, before, true); 

		if (r.second){
	    	localEventBus.postAsync(updateEntityEvent(before, r.first));			
		}
		return r.first;
	}
	
	public void fetchAll(final int batchSize, Consumer<List<ENTITY>> consumer){
		
		final List<ENTITY> entities =  getDao().findByCriteria(Restrictions.sqlRestriction("true"), null);
		
		for (List<ENTITY> batch : Lists.partition(entities, batchSize))
			consumer.accept(batch);
    }
}

