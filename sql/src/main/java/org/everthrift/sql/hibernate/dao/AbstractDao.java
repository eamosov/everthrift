package org.everthrift.sql.hibernate.dao;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.everthrift.appserver.model.DaoEntityIF;
import org.everthrift.appserver.model.UniqueException;
import org.everthrift.utils.Pair;
import org.hibernate.LockMode;
import org.hibernate.Query;
import org.hibernate.SQLQuery;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.criterion.Criterion;
import org.hibernate.criterion.Order;
import org.hibernate.criterion.Projection;
import org.hibernate.transform.ResultTransformer;
import org.hibernate.type.Type;

import com.google.common.base.Function;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;

public interface AbstractDao<K, V extends DaoEntityIF> {

    public Class<V> getEntityClass();

    public V findById(K id);

    public Collection<V> findByIds(Collection<K> id);

    /**
     * Если entity не найдено, то метод возвращает null для этого ключа
     * @param id
     * @param keyExtractor
     * @return
     */
    public Map<K, V> findByIdsAsMap(Collection<K> id);

    public void persist(V e) throws UniqueException;

    public Pair<V, Boolean> saveOrUpdate(V e) throws UniqueException;

    public Pair<V, Boolean> save(V e) throws UniqueException;

    public void delete(V e);

    public void deleteAll();

    public void evict(K id);

    public Object uniqueResult(Criterion criterion, Projection... projections);

    public List<K> findPkByCriteria(Criterion criterion, Order order);

    public List<V> findByCriteria(Criterion criterion, Order order);

    public List<V> findByCriteria(Criterion criterion, Projection proj, LockMode lockMode, List<Order> order, Integer limit,
                                  Integer offset);

    public ListenableFuture<List<V>> findByCriteriaAsync(Criterion criterion, Order order);

    public V findFirstByCriteria(Criterion criterion, Order order);

    public ListenableFuture<V> findFirstByCriteriaAsync(Criterion criterion, Order order);

    public <X> List<X> findBySQLQuery(String query, Map<String, Type> mapping, ResultTransformer resultTransformer, Object... params);

    public int executeCustomUpdate(K evictId, String query, Object... params);

    public int executeCustomUpdate(K evictId, String query, Function<SQLQuery, Query> bindFunction);

    public void setSessionFactory(SessionFactory sessionFactory);

    public Session getCurrentSession();

    public void setListeningExecutorService(ListeningExecutorService listeningExecutorService);

    public AbstractDao<K, V> with(SessionFactory sessionFactory);
}
