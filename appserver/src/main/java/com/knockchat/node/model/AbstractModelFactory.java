package com.knockchat.node.model;

import static org.springframework.data.mongodb.core.query.Criteria.where;
import static org.springframework.data.mongodb.core.query.Query.query;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.SortedSet;
import java.util.concurrent.Callable;

import org.apache.commons.lang.NotImplementedException;
import org.apache.thrift.TException;
import org.hibernate.SessionFactory;
import org.hibernate.StaleObjectStateException;
import org.hibernate.exception.ConstraintViolationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.transaction.annotation.Transactional;

import com.google.common.base.Throwables;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.knockchat.appserver.model.CreatedAtIF;
import com.knockchat.appserver.model.UpdatedAtIF;
import com.knockchat.hibernate.dao.AbstractDao;
import com.knockchat.hibernate.dao.AbstractDaoImpl;
import com.knockchat.hibernate.dao.DaoEntityIF;
import com.knockchat.sql.objects.ObjectStatements;
import com.knockchat.utils.Pair;

public abstract class AbstractModelFactory<PK extends Serializable, ENTITY extends DaoEntityIF<ENTITY>> implements InitializingBean {
	
	public static final Logger log = LoggerFactory.getLogger(AbstractModelFactory.class);
	
	public static enum Storage{
		PGSQL,
		MONGO
	}

    @Autowired
    protected ObjectStatements objectStatements;

    @Autowired
    protected List<SessionFactory> sessionFactories;
    
    @Autowired
    private ApplicationContext ctx;
    
    protected final Class<ENTITY> entityClass;
    protected final Storage storage;
    
    private AbstractDao<PK, ENTITY> dao;
    private MongoTemplate mongo;

    @Autowired
    protected ListeningExecutorService listeningExecutorService;
    
    private ThreadLocal<Boolean> isUpdated = new ThreadLocal<Boolean>(){
    	@Override
        protected Boolean initialValue() {
            return false;
        }
    }; 

    protected AbstractModelFactory() {
        this(null, null);
    }

    protected AbstractModelFactory(Class<ENTITY> entityClass, Storage storage) {
        this.entityClass = entityClass;
        this.storage = storage;
        
        if (storage == Storage.PGSQL){
        	dao = new AbstractDaoImpl<PK, ENTITY>(this.entityClass);
        }
    }

    @Override
    public void afterPropertiesSet() throws Exception {
    	if (storage == Storage.PGSQL){
        	
        	SessionFactory sf = null;
        	
            for (SessionFactory factory : sessionFactories)
                if (factory.getClassMetadata(this.entityClass) != null){
                	sf = factory;
                	break;
                }

            if (sf == null)
            	throw new RuntimeException("Cound't find SessionFactory for class " + this.entityClass.getSimpleName());
            	            
            dao.setSessionFactory(sf);            
            dao.setListeningExecutorService(listeningExecutorService);
        }else if (storage == Storage.MONGO){
        	mongo = ctx.getBean(MongoTemplate.class);
        }
    }
    
    public boolean isUpdated(){
    	return isUpdated.get();
    }
    
    public ENTITY updateEntity(ENTITY e) {
    	
    	final long now = System.currentTimeMillis() / 1000;
    	
        if (e instanceof UpdatedAtIF)
            ((UpdatedAtIF) e).setUpdatedAt(now);
        
        if (e instanceof CreatedAtIF && e.getPk() == null)
        	((CreatedAtIF)e).setCreatedAt(now);

    	if (storage == Storage.PGSQL){
    		try{
    			final Pair<ENTITY, Boolean> ret = dao.saveOrUpdate(e);
    			isUpdated.set(ret.second);
    			return ret.first;
    		}catch(RuntimeException e1){
    			isUpdated.set(false);
    			throw e1;
    		}
    	}else if (storage == Storage.MONGO){
        	mongo.save(e);
        	isUpdated.set(true);
        	final ENTITY ret = mongo.findById(e.getPk(), entityClass);
        	
        	if (ret == null)
        		throw new RuntimeException("null after save: " + e.toString());
        	
        	return ret;
    	}else{
    		throw new NotImplementedException("no storage class");
    	}
    }
    
    public ENTITY findEntityById(PK id){
    	if (storage == Storage.PGSQL)
    		return dao.findById(id);
    	else if (storage == Storage.MONGO)
    		return mongo.findById(id, entityClass);
    	else
    		throw new NotImplementedException("no storage class");
    }

    public Collection<ENTITY> findEntityById(Collection<PK> ids){
    	if (storage == Storage.PGSQL)
    		return dao.findByIds(ids);
    	else if (storage == Storage.MONGO)
    		return mongo.find(query(where("_id").in(ids)), entityClass);
    	else
    		throw new NotImplementedException("no storage class");
    }
    
    public Map<PK, ENTITY> findEntityByIdAsMap(Collection<PK> id) {
        final Map<PK, ENTITY> ret = new HashMap<PK, ENTITY>();
        final Collection<ENTITY> rs = findEntityById(id);
        for (ENTITY r : rs) {
            ret.put((PK)r.getPk(), r);
        }
        for (PK k : id)
            if (!ret.containsKey(k))
                ret.put(k, null);
        return ret;
    }

    public void deleteEntity(ENTITY e){
    	if (storage == Storage.PGSQL)
    		dao.delete(e);
    	else if (storage == Storage.MONGO)
    		mongo.remove(e);
    	else
    		throw new NotImplementedException("no storage class");
    }
        
    public AbstractDao<PK, ENTITY> getDao(){
    	if (storage == Storage.PGSQL)
    		return dao;
    	else
    		throw new NotImplementedException("not a PGSQL storage");
    }
    
    public MongoTemplate getMongo(){
    	if (storage == Storage.MONGO)
    		return mongo;
    	else
    		throw new NotImplementedException("not a MONGO storage");    	
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public void update(List<ENTITY> l) {
        for (ENTITY e : (SortedSet<ENTITY>) (SortedSet) Sets.newTreeSet((List<Comparable<?>>) l))
        	update(e);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public void update(ENTITY[] l) {
        final SortedSet<ENTITY> s = (SortedSet<ENTITY>) (SortedSet) Sets.newTreeSet();
        for (ENTITY e : l)
            s.add(e);

        for (ENTITY e : l)
        	update(e);
    }
    
    public ENTITY update(ENTITY e){
    	throw new NotImplementedException("factory should implement update()");
    }

    protected <T> T optimisticUpdate(Callable<T> updateFunction) throws Exception{
    	return optimisticUpdate(updateFunction, 5, 100);
    }
    
    protected <T> T optimisticUpdate(Callable<T> updateFunction, int maxIteration, int maxTimeoutMillis) throws Exception{
			int i=0;
			T updated = null;
			do{
				updated = updateFunction.call();
				
				i++;
				if (updated == null)
					try {
						Thread.sleep(new Random().nextInt(maxTimeoutMillis));
					} catch (InterruptedException e) {
					}					
			}while(updated == null && i<maxIteration);
			
			return updated;
    }
    
    public static class EntityNotFoundException extends Exception{
    	
		private static final long serialVersionUID = 1L;
		
		public final Object id;
    
    	public EntityNotFoundException(Object id){
    		this.id = id;
    	}
    }
    
    public static interface Mutator<ENTITY>{

    	/**
    	 * Перед транзакцией
    	 * @return true = обновить в базе, false - не обновлять в базе и успешно завершить обновление 
    	 */
    	boolean beforeUpdate() throws Exception;
    	
    	/**
    	 *	Вызывается внутри транзакции 
    	 * @param e
    	 * @return  true = обновить в базе, false - не обновлять в базе и успешно завершить обновление
    	 */
    	boolean update(ENTITY e) throws Exception;

    	/**
    	 * После транзакции в finally блоке
    	 */
    	void afterUpdate();
    }
    
    public static abstract class BasicMutator<ENTITY> implements Mutator<ENTITY>{

		@Override
		public boolean beforeUpdate() throws Exception {
			return true;
		}

		@Override
		public void afterUpdate() {
		}
    	
    }
    
    public static abstract class MutatorDecorator<ENTITY> implements Mutator<ENTITY>{
    	
    	protected final Mutator<ENTITY> delegate;
    	
    	public MutatorDecorator(Mutator<ENTITY> e){
    		delegate = e;
    	}    	
    }
    
    public static interface Factory<PK, ENTITY>{
    	ENTITY create(PK id);
    }
    
	@Transactional(rollbackFor=Exception.class)
	private Pair<ENTITY,ENTITY> tryOptimisticUpdate(PK id, Mutator<ENTITY> mutator, final Factory<PK, ENTITY> factory) throws Exception, EntityNotFoundException{
		
		ENTITY e;
		ENTITY orig = null;
		
		e = findEntityById(id);
		if (e == null){
			if (factory == null)
				throw new EntityNotFoundException(id);
			
			e = factory.create(id);
			getDao().persist(e);
		}
		
		try {
			orig = this.entityClass.getConstructor(this.entityClass).newInstance(e);
		} catch (InstantiationException | IllegalAccessException
				| IllegalArgumentException | InvocationTargetException
				| NoSuchMethodException | SecurityException e1) {
			Throwables.propagate(e1);
		}
		
		if (mutator.update(e))
			e = updateEntity(e);
				
		return Pair.create(e, orig);
	}
	
	public Pair<ENTITY,ENTITY> optimisticUpdate__(final PK id, final Mutator<ENTITY> mutator) throws EntityNotFoundException{
		try {
			return optimisticUpdate(id, mutator, null);
		} catch (EntityNotFoundException e){
			throw e;
		} catch (Exception e) {
			throw Throwables.propagate(e);
		}
	}

	public Pair<ENTITY,ENTITY> optimisticUpdate__(final PK id, final Mutator<ENTITY> mutator, final Factory<PK, ENTITY> factory){
		try {
			return optimisticUpdate(id, mutator, factory);
		} catch (Exception e) {
			throw Throwables.propagate(e);
		}
	}
	
	/**
	 * 
	 * @param id
	 * @param mutator
	 * @param factory
	 * @return <new, old>
	 * @throws Exception
	 */
	public Pair<ENTITY,ENTITY> optimisticUpdate(final PK id, final Mutator<ENTITY> mutator, final Factory<PK, ENTITY> factory) throws TException, Exception {
		
		return optimisticUpdate(new Callable<Pair<ENTITY,ENTITY>>(){

			@Override
			public Pair<ENTITY,ENTITY> call() throws Exception {
				try{
					if (mutator.beforeUpdate())
						return tryOptimisticUpdate(id, mutator, factory);
					else
						return Pair.create(null,  null);
				}catch(StaleObjectStateException | ConstraintViolationException e){
					log.debug("update fails id= " + id + ", let's try one more time?", e);
					return null;
				}finally{
					mutator.afterUpdate();
				}
			}}, 10, 100);		
	}
    
}
