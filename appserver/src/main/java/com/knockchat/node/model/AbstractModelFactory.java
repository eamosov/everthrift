package com.knockchat.node.model;

import static org.springframework.data.mongodb.core.query.Criteria.where;
import static org.springframework.data.mongodb.core.query.Query.query;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;

import org.apache.commons.lang.NotImplementedException;
import org.hibernate.SessionFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.data.mongodb.core.MongoTemplate;

import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.knockchat.hibernate.dao.AbstractDao;
import com.knockchat.hibernate.dao.AbstractDaoImpl;
import com.knockchat.hibernate.dao.DaoEntityIF;
import com.knockchat.sql.objects.ObjectStatements;

public abstract class AbstractModelFactory<PK extends Serializable, ENTITY extends DaoEntityIF<ENTITY>> implements InitializingBean {
	
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
    
    public ENTITY updateEntity(ENTITY e) {
    	if (storage == Storage.PGSQL){
    		return dao.saveOrUpdate(e);
    	}else if (storage == Storage.MONGO){
        	mongo.save(e);
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

}
