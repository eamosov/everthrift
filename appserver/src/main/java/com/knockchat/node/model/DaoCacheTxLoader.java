package com.knockchat.node.model;

import java.util.Collection;

import net.sf.ehcache.CacheException;

import org.springframework.transaction.annotation.Transactional;

import com.google.common.base.Function;
import com.knockchat.hibernate.dao.AbstractDao;
import com.knockchat.hibernate.dao.DaoEntityIF;

public class DaoCacheTxLoader<K, V extends DaoEntityIF<V>> extends DaoCacheLoader<K, V> {

	public DaoCacheTxLoader(String name, AbstractDao<K, V> dao, Function<V, K> keyExtractor) {
		super(name, dao, keyExtractor);
	}

	public DaoCacheTxLoader(String name, AbstractDao<K, V> dao) {
		super(name, dao);
	}

    @Override
    @Transactional
    protected Collection<V> loadImpl(Collection<K> keys) {
    	return super.loadImpl(keys);
    }

    @Override
    @Transactional
    protected V loadImpl(K key) throws CacheException {
        return super.loadImpl(key);
    }				
	
}
