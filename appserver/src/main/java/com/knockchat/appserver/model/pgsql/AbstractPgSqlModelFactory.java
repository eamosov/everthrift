package com.knockchat.appserver.model.pgsql;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.annotation.PostConstruct;

import org.apache.thrift.TException;
import org.hibernate.SessionFactory;
import org.hibernate.criterion.Order;
import org.hibernate.criterion.Projections;
import org.hibernate.criterion.Restrictions;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import com.google.common.util.concurrent.ListeningExecutorService;
import com.knockchat.appserver.model.AbstractCachedModelFactory;
import com.knockchat.appserver.model.LocalEventBus;
import com.knockchat.appserver.model.RwModelFactoryIF;
import com.knockchat.hibernate.dao.AbstractDao;
import com.knockchat.hibernate.dao.AbstractDaoImpl;
import com.knockchat.hibernate.dao.DaoEntityIF;

import net.sf.ehcache.Cache;

public abstract class AbstractPgSqlModelFactory<PK extends Serializable, ENTITY extends DaoEntityIF, E extends TException> extends AbstractCachedModelFactory<PK, ENTITY> implements RwModelFactoryIF<PK, ENTITY, E> {
		
    @Autowired
    protected SessionFactory sessionFactory;

    @Autowired
    @Qualifier("listeningCallerRunsBoundQueueExecutor")
    private ListeningExecutorService listeningExecutorService;
    
	@Autowired
	protected LocalEventBus localEventBus;


    private final AbstractDao<PK, ENTITY> dao;
    protected final Class<ENTITY> entityClass;
    
//    final RwModelFactoryHelper<PK, ENTITY> helper;
//    
    protected abstract E createNotFoundException(PK id);
    
    protected AbstractPgSqlModelFactory(Cache cache, Class<ENTITY> entityClass, ListeningExecutorService listeningExecutorService, SessionFactory sessionFactory, LocalEventBus localEventBus) {
    	super(cache);
    	
    	this.entityClass = entityClass;     	
       	dao = new AbstractDaoImpl<PK, ENTITY>(this.entityClass);
//       	helper = new RwModelFactoryHelper<PK, ENTITY>(){
//
//			@Override
//			protected ENTITY updateEntityImpl(ENTITY e) {
//				return AbstractPgSqlModelFactory.this.updateEntityImpl(e);
//			}
//
//			@Override
//			protected PK extractPk(ENTITY e) {
//				return (PK)e.getPk();
//			}};
			
		this.listeningExecutorService = listeningExecutorService;
		this.localEventBus = localEventBus;
		this.sessionFactory = sessionFactory;
		_afterPropertiesSet();
    }
        
    protected AbstractPgSqlModelFactory(String cacheName, Class<ENTITY> entityClass) {
    	
    	super(cacheName);
    	
    	this.entityClass = entityClass;     	
       	dao = new AbstractDaoImpl<PK, ENTITY>(this.entityClass);
//       	helper = new RwModelFactoryHelper<PK, ENTITY>(){
//
//			@Override
//			protected ENTITY updateEntityImpl(ENTITY e) {
//				return AbstractPgSqlModelFactory.this.updateEntityImpl(e);
//			}
//
//			@Override
//			protected PK extractPk(ENTITY e) {
//				return (PK)e.getPk();
//			}};
    }
    
    private void _afterPropertiesSet(){
    	        	            
        dao.setSessionFactory(sessionFactory);            
        dao.setListeningExecutorService(listeningExecutorService);
        
        localEventBus.register(this);
    }

    @PostConstruct
    private void afterPropertiesSet() {
    	_afterPropertiesSet();
    }
        
//    private ENTITY updateEntityImpl(ENTITY e) {
//    	    		
//		try{
//			final Pair<ENTITY, Boolean> ret = dao.saveOrUpdate(e);
//			
//			if (ret.second){
//				doAfterCommit( () -> {_invalidate((PK)e.getPk());} );
//			}
//			
//			helper.setUpdated(ret.second);
//			return ret.first;
//		}catch(Exception e1){
//			helper.setUpdated(false);
//			throw e1;
//		}				
//    }
    
    protected final void _invalidateEhCache(PK id){
    	super.invalidate(id);
    }
    
    @Override
    public final void invalidate(PK id){
    	_invalidateEhCache(id);
    	getDao().evict(id);
    }
    
    @Override
    public final void invalidateLocal(PK id){
    	super.invalidateLocal(id);
    	getDao().evict(id);
    }
    
    @Override
    protected ENTITY fetchEntityById(PK id){
    	return dao.findById(id);
    }

    @Override
    protected Map<PK, ENTITY> fetchEntityByIdAsMap(Collection<PK> ids){
    	
    	if (getCache() == null)
    		log.warn("fetch by collection, but cache is null");
    	
    	return dao.findByIdsAsMap(ids);
    }
    
    @Override
    public final void deleteEntity(ENTITY e) throws E{
		final PK pk = (PK)e.getPk();
    	final ENTITY _e = fetchEntityById(pk);
    	if (_e ==null)
    		throw createNotFoundException(pk);

		dao.delete(_e);
    	_invalidateEhCache(pk);
    	
    	localEventBus.postAsync(deleteEntityEvent(_e));
    }
        
    public final AbstractDao<PK, ENTITY> getDao(){
   		return dao;
    }
    
    public final Iterator<PK> getAllIds(String pkName){
    	return ((List)getDao().findByCriteria(Restrictions.and(), Projections.property(pkName), null, Collections.singletonList(Order.asc(pkName)), null, null)).iterator();	
    }
    
	@Override
	public final Class<ENTITY> getEntityClass() {
		return this.entityClass;
	}

}
