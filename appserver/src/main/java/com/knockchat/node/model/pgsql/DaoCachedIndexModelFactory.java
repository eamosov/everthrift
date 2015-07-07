package com.knockchat.node.model.pgsql;

import java.util.Collection;
import java.util.Collections;

import net.sf.ehcache.Cache;

import org.hibernate.criterion.Criterion;
import org.hibernate.criterion.Order;
import org.hibernate.criterion.Projections;

import com.knockchat.hibernate.dao.AbstractDao;
import com.knockchat.node.model.CachedIndexModelFactory;

public abstract class DaoCachedIndexModelFactory<K, V> extends CachedIndexModelFactory<K, V> {
	
	private final AbstractDao dao;

	public DaoCachedIndexModelFactory(String cacheName, AbstractDao dao) {
		super(cacheName);
		this.dao = dao;
	}

	public DaoCachedIndexModelFactory(Cache cache, AbstractDao dao) {
		super(cache);
		this.dao = dao;
	}

	@Override
	protected Collection<Object[]> loadImpl(Collection<K> keys) {		
		return dao.findByCriteria(getCriterion(keys), Projections.projectionList().add(Projections.property(getPkProperty())).add(Projections.property(getIndexedProperty())), null, Collections.singletonList(getOrder()), null, null);
	}
	
	protected abstract Criterion getCriterion(Collection<K> keys);
	protected abstract String getPkProperty();
	protected abstract String getIndexedProperty();
	protected abstract Order getOrder();
}
