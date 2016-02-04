package com.knockchat.node.model.pgsql;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.hibernate.SessionFactory;
import org.hibernate.criterion.Order;
import org.hibernate.criterion.Projections;
import org.hibernate.criterion.Restrictions;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.transaction.annotation.Transactional;

import com.google.common.util.concurrent.ListeningExecutorService;
import com.knockchat.hibernate.dao.AbstractDao;
import com.knockchat.hibernate.dao.AbstractDaoImpl;
import com.knockchat.hibernate.dao.DaoEntityIF;
import com.knockchat.node.model.AbstractCachedModelFactory;
import com.knockchat.node.model.RwModelFactoryHelper;
import com.knockchat.node.model.RwModelFactoryIF;
import com.knockchat.utils.Pair;

import net.sf.ehcache.Cache;

public class AbstractPgSqlModelFactory<PK extends Serializable, ENTITY extends DaoEntityIF> extends AbstractCachedModelFactory<PK, ENTITY> implements InitializingBean, RwModelFactoryIF<PK, ENTITY> {
		
    @Autowired
    protected List<SessionFactory> sessionFactories;
    
    protected SessionFactory sessionFactory;

    @Autowired
    protected ListeningExecutorService listeningExecutorService;

    private final AbstractDao<PK, ENTITY> dao;
    protected final Class<ENTITY> entityClass;
    
    final RwModelFactoryHelper<PK, ENTITY> helper;
    
    protected AbstractPgSqlModelFactory(Cache cache, Class<ENTITY> entityClass, ListeningExecutorService listeningExecutorService, List<SessionFactory> sessionFactories) {
    	super(cache);
    	
    	this.entityClass = entityClass;     	
       	dao = new AbstractDaoImpl<PK, ENTITY>(this.entityClass);
       	helper = new RwModelFactoryHelper<PK, ENTITY>(){

			@Override
			protected ENTITY updateEntityImpl(ENTITY e) {
				return AbstractPgSqlModelFactory.this.updateEntityImpl(e);
			}

			@Override
			protected PK extractPk(ENTITY e) {
				return (PK)e.getPk();
			}};
			
		this.listeningExecutorService = listeningExecutorService;
		this.sessionFactories = sessionFactories;
		_afterPropertiesSet();
    }
        
    protected AbstractPgSqlModelFactory(String cacheName, Class<ENTITY> entityClass) {
    	
    	super(cacheName);
    	
    	this.entityClass = entityClass;     	
       	dao = new AbstractDaoImpl<PK, ENTITY>(this.entityClass);
       	helper = new RwModelFactoryHelper<PK, ENTITY>(){

			@Override
			protected ENTITY updateEntityImpl(ENTITY e) {
				return AbstractPgSqlModelFactory.this.updateEntityImpl(e);
			}

			@Override
			protected PK extractPk(ENTITY e) {
				return (PK)e.getPk();
			}};
    }
    
    private void _afterPropertiesSet(){
    	
        for (SessionFactory factory : sessionFactories)
            if (factory.getClassMetadata(this.entityClass) != null){
            	sessionFactory = factory;
            	break;
            }

        if (sessionFactory == null)
        	throw new RuntimeException("Cound't find SessionFactory for class " + this.entityClass.getSimpleName());
        	            
        dao.setSessionFactory(sessionFactory);            
        dao.setListeningExecutorService(listeningExecutorService);    	
    }

    @Override
    public void afterPropertiesSet() {
        	
    	super.afterPropertiesSet();
    	
    	_afterPropertiesSet();
    }
        
    private ENTITY updateEntityImpl(ENTITY e) {
    	    		
		try{
			final Pair<ENTITY, Boolean> ret = dao.saveOrUpdate(e);
			
			if (ret.second){
				doAfterCommit( () -> {_invalidate((PK)e.getPk());} );
			}
			
			helper.setUpdated(ret.second);
			return ret.first;
		}catch(Exception e1){
			helper.setUpdated(false);
			throw e1;
		}				
    }
    
    protected void _invalidate(PK id){
    	super.invalidate(id);
    }
    
    @Override
    public void invalidate(PK id){
    	_invalidate(id);
    	getDao().evict(id);
    }
    
    @Override
    public void invalidateLocal(PK id){
    	super.invalidateLocal(id);
    	getDao().evict(id);
    }
    
    @Override
    protected ENTITY fetchEntityById(PK id){
    	return dao.findById(id);
    }

    @Override
    protected Map<PK, ENTITY> fetchEntityByIdAsMap(Collection<PK> ids){
    	
    	if (this.cache == null)
    		log.error("fetch by collection, but cache is null");
    	
    	return dao.findByIdsAsMap(ids);
    }
    
    @Override
    @Transactional
    public void deleteEntity(ENTITY e){
    	@SuppressWarnings("unchecked")
		final PK pk = (PK)e.getPk();
    	final ENTITY _e = dao.findById(pk);
    	if (_e !=null){
    		dao.delete(_e);
    	}
    	
    	doAfterCommit(()->{_invalidate(pk);});   		
    }
        
    public AbstractDao<PK, ENTITY> getDao(){
   		return dao;
    }
    
    public Iterator<PK> getAllIds(){
    	return ((List)getDao().findByCriteria(Restrictions.and(), Projections.property("id"), null, Collections.singletonList(Order.asc("id")), null, null)).iterator();	
    }
       
	@Override
	public ENTITY insertEntity(ENTITY e) {
		return helper.updateEntity(e);
	}
	
	public void deleteAll(){
		getDao().deleteAll();
		
		if (cache !=null)
			cache.removeAll();
	}

	@Override
	public Class<ENTITY> getEntityClass() {
		return this.entityClass;
	}	
}
