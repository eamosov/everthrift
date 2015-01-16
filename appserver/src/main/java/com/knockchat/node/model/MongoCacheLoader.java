package com.knockchat.node.model;

import static org.springframework.data.mongodb.core.query.Criteria.where;
import static org.springframework.data.mongodb.core.query.Query.query;

import java.io.Serializable;
import java.util.Collection;

import org.springframework.data.mongodb.core.MongoTemplate;

import com.google.common.base.Function;
import com.knockchat.hibernate.dao.DaoEntityIF;

public class MongoCacheLoader<K, V extends DaoEntityIF<V>> extends AbstractCacheLoader<K,V> {
	
	private final MongoTemplate mongo;
	private final Class<V> cls;

	public MongoCacheLoader(String name, MongoTemplate mongo, Class<V> cls, Function<V,K> keyExtractor) {
		super(name, keyExtractor);
		this.mongo = mongo;
		this.cls = cls;
	}

	public MongoCacheLoader(String name, MongoTemplate mongo, Class<V> cls) {
		super(name, MongoCacheLoader.<K,V>keyExtractor());
		this.mongo = mongo;
		this.cls = cls;
	}
	    
	@Override
	protected Collection<V> loadImpl(Collection<K> keys) {				
		return mongo.find(query(where("_id").in(keys)), cls);
	}

	@Override
	protected V loadImpl(K key) {
		return mongo.findById(key, cls);
	};

    @SuppressWarnings("rawtypes")
	public static final Function<DaoEntityIF, Serializable> keyExtractor = new Function<DaoEntityIF, Serializable>() {
        @Override
        public Serializable apply(DaoEntityIF input) {
            return input.getPk();
        }
    };
    
    @SuppressWarnings("unchecked")
	public static <K, V extends DaoEntityIF<V>> Function<V,K> keyExtractor(){
    	return (Function<V,K>)keyExtractor;
    }
}
