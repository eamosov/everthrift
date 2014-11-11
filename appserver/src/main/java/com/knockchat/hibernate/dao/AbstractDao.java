package com.knockchat.hibernate.dao;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.hibernate.LockMode;
import org.hibernate.SessionFactory;
import org.hibernate.criterion.Criterion;
import org.hibernate.criterion.Order;
import org.hibernate.criterion.Projection;
import org.hibernate.transform.ResultTransformer;
import org.hibernate.type.Type;

import com.google.common.base.Function;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;


public interface AbstractDao<K, V extends DaoEntityIF<V>> {

    public V findById(K id);

    public Collection<V> findByIds(Collection<K> id);

    public Map<K, V> findByIdsAsMap(Collection<K> id, Function<V, K> keyExtractor);

    public V saveOrUpdate(V e);

    public void delete(V e);

    public Object uniqueResult(Criterion criterion, Projection... projections);

    public List<V> findByCriteria(Criterion criterion, Order order);

    public List<V> findByCriteria(Criterion criterion, LockMode lockMode, Order order, int limit, int offset);

    public ListenableFuture<List<V>> findByCriteriaAsync(Criterion criterion, Order order);

    public V findFirstByCriteria(Criterion criterion, Order order);

    public ListenableFuture<V> findFirstByCriteriaAsync(Criterion criterion, Order order);

    public <X> List<X> findBySQLQuery(String query, Map<String, Type> mapping, ResultTransformer resultTransformer, Object... params);

    public int executeCustomUpdate(String query, Object... params);

    public void setSessionFactory(SessionFactory sessionFactory);

    public void setListeningExecutorService(ListeningExecutorService listeningExecutorService);

    public AbstractDao<K, V> with(SessionFactory sessionFactory);
}
