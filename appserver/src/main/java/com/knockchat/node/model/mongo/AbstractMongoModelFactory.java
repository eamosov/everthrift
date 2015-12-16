package com.knockchat.node.model.mongo;

import static org.springframework.data.mongodb.core.query.Criteria.where;
import static org.springframework.data.mongodb.core.query.Query.query;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.data.mongodb.core.MongoTemplate;

import com.google.common.collect.Maps;
import com.knockchat.appserver.model.UpdatedAtIF;
import com.knockchat.hibernate.dao.DaoEntityIF;
import com.knockchat.node.model.AbstractCachedModelFactory;
import com.knockchat.node.model.ModelFactoryIF;
import com.knockchat.node.model.RwModelFactoryHelper;
import com.knockchat.utils.LongTimestamp;

public class AbstractMongoModelFactory<PK extends Serializable, ENTITY extends DaoEntityIF> extends AbstractCachedModelFactory<PK, ENTITY> implements InitializingBean, ModelFactoryIF<PK, ENTITY> {
	    
    @Autowired
    private ApplicationContext ctx;
    
    private MongoTemplate mongo;
    
    private final Class<ENTITY> entityClass;
    private final RwModelFactoryHelper<PK, ENTITY> helper;

    protected AbstractMongoModelFactory(String cacheName, Class<ENTITY> entityClass) {
    	super(cacheName);
    	this.entityClass = entityClass;
    	
    	helper = new RwModelFactoryHelper<PK, ENTITY>(){

			@Override
			protected ENTITY updateEntityImpl(ENTITY e) {
				return AbstractMongoModelFactory.this.updateEntityImpl(e);
			}

			@Override
			protected PK extractPk(ENTITY e) {
				return (PK)e.getPk();
			}};
    }

    @Override
    public void afterPropertiesSet(){
    	mongo = ctx.getBean(MongoTemplate.class);
    }
    
    private ENTITY updateEntityImpl(ENTITY e) {
    	
		if (e.getPk() !=null){
			//TODO надо обновлять UpdatedAtIF только если что-то реально изменилось
    		if (e instanceof UpdatedAtIF)
    			((UpdatedAtIF) e).setUpdatedAt(LongTimestamp.now());    			
		}

    	mongo.save(e);
    	helper.setUpdated(true);
    	invalidate((PK)e.getPk());
    	
    	final ENTITY ret = mongo.findById(e.getPk(), entityClass);
    	
    	if (ret == null)
    		throw new RuntimeException("null after save: " + e.toString());
    	
    	return ret;
    }
    
    @Override
    protected ENTITY fetchEntityById(PK id){
   		return mongo.findById(id, entityClass);
    }

	@Override
	protected Map<PK, ENTITY> fetchEntityByIdAsMap(Collection<PK> ids) {
		
		final List<ENTITY> entities =  mongo.find(query(where("_id").in(ids)), entityClass);
		final Map<PK, ENTITY> ret = Maps.newHashMapWithExpectedSize(ids.size());
		for (ENTITY e:entities){
			ret.put((PK)e.getPk(), e);
		}
		
		for (PK k: ids){
			if (!ret.containsKey(k))
				ret.put(k, null);
		}
		return ret;
	}
    
    @Override
    public void deleteEntity(ENTITY e){
   		mongo.remove(e);
    }
        
    protected MongoTemplate getMongo(){
   		return mongo;
    }

	@Override
	public boolean isUpdated() {
		return helper.isUpdated();
	}

	@Override
	public ENTITY update(ENTITY e) {
		return helper.updateEntity(e);
	}

	@Override
	public ENTITY insertEntity(ENTITY e) {
		return helper.updateEntity(e);
	}
}
