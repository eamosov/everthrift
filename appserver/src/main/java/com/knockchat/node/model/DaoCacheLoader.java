package com.knockchat.node.model;

import java.io.Serializable;
import java.util.Collection;

import net.sf.ehcache.CacheException;

import com.google.common.base.Function;
import com.knockchat.hibernate.dao.AbstractDao;
import com.knockchat.hibernate.dao.DaoEntityIF;

public class DaoCacheLoader<K, V extends DaoEntityIF<V>> extends AbstractCacheLoader<K,V> {
	
	private final AbstractDao<K,V> dao;

	public DaoCacheLoader(String name, AbstractDao<K,V> dao, Function<V,K> keyExtractor) {
		super(name, keyExtractor);
		this.dao = dao;
	}

	public DaoCacheLoader(String name, AbstractDao<K,V> dao) {
		super(name, DaoCacheLoader.<K,V>daoExtractor());
		this.dao = dao;
	}
	
    @Override
    protected Collection<V> loadImpl(Collection<K> keys) {
    	return dao.findByIds(keys);
    }

    protected V loadImpl(K key) throws CacheException {
        return dao.findById(key);
    }

    @SuppressWarnings("rawtypes")
	public static final Function<DaoEntityIF, Serializable> daoExtractor = new Function<DaoEntityIF, Serializable>() {
        @Override
        public Serializable apply(DaoEntityIF input) {
            return input.getPk();
        }
    };
    
    @SuppressWarnings("unchecked")
	public static <K, V extends DaoEntityIF<V>> Function<V,K> daoExtractor(){
    	return (Function<V,K>)daoExtractor;
    }
}
