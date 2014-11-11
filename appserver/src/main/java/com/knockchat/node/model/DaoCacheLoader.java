package com.knockchat.node.model;

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;

import net.sf.ehcache.CacheException;

import com.google.common.base.Function;
import com.knockchat.hibernate.dao.AbstractDao;
import com.knockchat.hibernate.dao.DaoEntityIF;

public class DaoCacheLoader extends AbstractCacheLoader {
	
	private final AbstractDao dao;
    private final Function keyExtractor;

	public DaoCacheLoader(String name, AbstractDao dao, Function keyExtractor) {
		super(name);
		this.dao = dao;
        this.keyExtractor = keyExtractor;
	}
	
    @Override
    protected Map loadAllImpl(Collection keys) {
    	return dao.findByIdsAsMap(keys,keyExtractor);
    }

    public Object load(Object key) throws CacheException {
        return dao.findById(key);
    }

    public static final Function<DaoEntityIF, Serializable> daoExtractor = new Function<DaoEntityIF, Serializable>() {
        @Override
        public Serializable apply(DaoEntityIF input) {
            return input.getPk();
        }
    };

}
