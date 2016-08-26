package org.everthrift.sql.pgsql;

import net.sf.ehcache.Cache;
import org.everthrift.appserver.model.CachedIndexModelFactory;
import org.everthrift.sql.hibernate.dao.AbstractDao;
import org.hibernate.criterion.Criterion;
import org.hibernate.criterion.Order;
import org.hibernate.criterion.Projections;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

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
        return dao.findByCriteria(getCriterion(keys),
                                  Projections.projectionList().add(Projections.property(getPkProperty()))
                                             .add(Projections.property(getIndexedProperty())),
                                  null, Collections.singletonList(getOrder()), null, null);
    }

    protected abstract Criterion getCriterion(Collection<K> keys);

    protected abstract String getPkProperty();

    protected abstract String getIndexedProperty();

    protected abstract Order getOrder();

    @Override
    public Class<List<V>> getEntityClass() {
        return dao.getEntityClass();
    }

}
