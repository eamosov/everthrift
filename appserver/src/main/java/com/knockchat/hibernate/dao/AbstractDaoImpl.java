package com.knockchat.hibernate.dao;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import org.apache.commons.collections.CollectionUtils;
import org.hibernate.Criteria;
import org.hibernate.HibernateException;
import org.hibernate.LockMode;
import org.hibernate.Query;
import org.hibernate.SQLQuery;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.StatelessSession;
import org.hibernate.criterion.Criterion;
import org.hibernate.criterion.Order;
import org.hibernate.criterion.Projection;
import org.hibernate.criterion.ProjectionList;
import org.hibernate.criterion.Projections;
import org.hibernate.criterion.Restrictions;
import org.hibernate.persister.entity.AbstractEntityPersister;
import org.hibernate.transform.ResultTransformer;
import org.hibernate.type.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.transaction.annotation.Transactional;

import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.knockchat.utils.Pair;


public class AbstractDaoImpl<K extends Serializable, V extends DaoEntityIF> implements AbstractDao<K, V> {
	
	private static final Logger log = LoggerFactory.getLogger(AbstractDaoImpl.class);

    private static final String NO_SESSION_FOR_THREAD = "No Session found for current thread";

    private SessionFactory sessionFactory;
    protected final Class<V> entityClass;
	private  ListeningExecutorService listeningExecutorService;

    public AbstractDaoImpl(Class<V> entityClass) {
        this.entityClass = entityClass;
    }

    private AbstractDaoImpl(SessionFactory sessionFactory, Class<V> entityClass, ListeningExecutorService listeningExecutorService) {
        this.sessionFactory = sessionFactory;
        this.entityClass = entityClass;
        this.listeningExecutorService = listeningExecutorService;
    }

    @Override
	public void setSessionFactory(SessionFactory sessionFactory) {
        this.sessionFactory = sessionFactory;
    }

    @Override
    public Session getCurrentSession() {
        return sessionFactory.getCurrentSession();
    }

    private StatelessSession getStatelessSession() {
        return sessionFactory.openStatelessSession();
    }
    
	public ListeningExecutorService getListeningExecutorService() {
		return listeningExecutorService;
	}

	@Override
	public void setListeningExecutorService(ListeningExecutorService listeningExecutorService) {
		this.listeningExecutorService = listeningExecutorService;
	}    

    @Override
	public V findById(K id) {
    	
        try {
        	final Session session = getCurrentSession();
        	
        	if (log.isDebugEnabled())
        		log.debug("findById tx={}: {}#{}", session.getTransaction().getLocalStatus(), entityClass.getSimpleName(), id);
        	        	
            return (V) session.get(entityClass, id);
        } catch (HibernateException he) {
            propogateIfAbsentMessage(he, NO_SESSION_FOR_THREAD);
            final StatelessSession ss = getStatelessSession();
            try {
            	
                if (log.isDebugEnabled())
                	log.debug("findById stateless: {}#{}", entityClass.getSimpleName(), id);
            	
                return (V) ss.get(entityClass, id);
            } finally {
                ss.close();
            }
        }
    }

    @Override
	public Collection<V> findByIds(Collection<K> ids) {
        return this.findByCriteria(Restrictions.in("id",ids), null);
    }
    
    @Transactional
    @Override
    public void persist(V e){
    	final Session session =  getCurrentSession();    	
    	session.persist(e);
    }

    @Override
    @Transactional
	public Pair<V, Boolean> saveOrUpdate(V e) {
    	
    	if (log.isDebugEnabled())
    		log.debug("saveOrUpdate, class={}, object={}, id={}", e.getClass().getSimpleName(), System.identityHashCode(e), e.getPk());
    	
        final Session session = getCurrentSession();
                
        if (log.isDebugEnabled())
        	log.debug("saveOrUpdate tx={}: {}", session.getTransaction().getLocalStatus(), e);
        
        if (e.getPk() == null){
        	e.setPk(session.save(e));            	
        	session.refresh(e);
        	return Pair.create((V)session.get(entityClass, e.getPk()), true);
    	}else{
    		V ret = (V) session.merge(e);
    		if (session.isDirty()){    			    			
    			session.flush();
    		}
    		
            return Pair.create(ret, EntityInterceptor.INSTANCE.isDirty(e));
        }            
    }

    @Override
	public void delete(V e) {
        try {
            final Session session = getCurrentSession();
            
            if (log.isDebugEnabled())
            	log.debug("delete tx={}: {}", session.getTransaction().getLocalStatus(), e);

            session.delete(e);
        } catch (HibernateException he) {
            propogateIfAbsentMessage(he, NO_SESSION_FOR_THREAD);
            final StatelessSession ss = getStatelessSession();
            try {
                if (log.isDebugEnabled())
                	log.debug("delete stateless: {}", e);
            	
                ss.delete(e);
            } finally {
                ss.close();
            }
        }
    }
    
    @Override
    @Transactional
    public void deleteAll(){
        final Session session = getCurrentSession();        
    	session.createQuery("DELETE FROM " + ((AbstractEntityPersister)sessionFactory.getClassMetadata(entityClass)).getEntityName()).executeUpdate();    	
    }

    @Override
    public Object uniqueResult(Criterion criterion, Projection... projections) {
        Pair<Criteria, StatelessSession> pair = createCriteria();
        try {
            final Criteria criteria = pair.first;
            criteria.add(criterion);
            ProjectionList projectionList = Projections.projectionList();
            for (Projection p : projections)
            projectionList = projectionList.add(p);
            criteria.setProjection(projectionList);
            return criteria.uniqueResult();
        }
        finally {
           if (pair.second != null)
               pair.second.close();
        }
    }

    private Pair<Criteria, StatelessSession> createCriteria(){
        StatelessSession ss = null;
        Criteria criteria;
        try {
            final Session session = getCurrentSession();

            criteria = session.createCriteria(entityClass);

            if (log.isDebugEnabled())
                log.debug("createCriteria tx={}: {}", session.getTransaction().getLocalStatus(), entityClass.getSimpleName());

        } catch (HibernateException he) {
            propogateIfAbsentMessage(he, NO_SESSION_FOR_THREAD);
            ss = getStatelessSession();
            criteria = ss.createCriteria(entityClass);

            if (log.isDebugEnabled())
                log.debug("Create stateless session: {}", entityClass.getSimpleName());

        }
        return new Pair<Criteria, StatelessSession>(criteria, ss);
    }
    
	@Override
	public List<K> findPkByCriteria(Criterion criterion, Order order) {
		return (List)findByCriteria(criterion, Projections.property("id"), null, order !=null ? Collections.singletonList(order): null, null, null);
	}

    @Override
	public List<V> findByCriteria(Criterion criterion, Order order) {
    	return findByCriteria(criterion, null, null, order !=null ? Collections.singletonList(order): null, null, null);
    }    

    @SuppressWarnings("unchecked")
	@Override
    public List<V> findByCriteria(Criterion criterion, Projection proj, LockMode lockMode, List<Order> order, Integer limit, Integer offset) {
        final Pair<Criteria, StatelessSession> pair = createCriteria();
        try {
            final Criteria criteria = pair.first;
            criteria.add(criterion);
            
            if (!CollectionUtils.isEmpty(order))
            	for (Order o: order)
            		if (o!=null)
            			criteria.addOrder(o);
            
            if (proj !=null)
            	criteria.setProjection(proj);
            
            if (lockMode != null)
                criteria.setLockMode(lockMode);
            
            if (limit !=null)
            	criteria.setMaxResults(limit);
            
            if (offset !=null)
            	criteria.setFirstResult(offset);
            return Lists.newArrayList(criteria.list());
        }
        finally {
            if (pair.second != null)
                pair.second.close();
        }
    }

    /**
     * @param query             sql query : "?" - param placeholder
     * @param mapping           ResultSet mapping --> columnName -> columnType, may be null
     * @param resultTransformer transformer for resultSet, may be null
     * @param params            query params
     * @return
     * @see org.hibernate.transform.Transformers
     * Если не передан параметр mapping то будет использоваться маппер и трансформер класса entityClass
     */
    @Override
	public <X> List<X> findBySQLQuery(String query,  Map<String, Type> mapping, ResultTransformer resultTransformer, Object... params) {
        StatelessSession ss = null;

        try {
            SQLQuery q;
            try {
            	final Session session =  getCurrentSession();
                q = session.createSQLQuery(query);
                
                if (log.isDebugEnabled())
                	log.debug("findBySQLQuery tx={}: {}#{}", session.getTransaction().getLocalStatus(), entityClass.getSimpleName(), query);                                
                
            } catch (HibernateException he) {
                propogateIfAbsentMessage(he, NO_SESSION_FOR_THREAD);
                ss = getStatelessSession();
                q = ss.createSQLQuery(query);
                
                if (log.isDebugEnabled())
                	log.debug("findBySQLQuery stateless: {}#{}", entityClass.getSimpleName(), query);                                
                
            }
            for (int i = 0; i < params.length; i++) {
                q.setParameter(i, params[i]);
            }
            if (mapping != null) {
                for (String columnName : mapping.keySet())
                    q.addScalar(columnName, mapping.get(columnName));
            } else{
                q.addEntity(entityClass);
            }
            if (resultTransformer != null && mapping != null)
                q.setResultTransformer(resultTransformer);
            return q.list();
        } finally {
            if (ss != null)
                ss.close();
        }
    }

    @Override
    public int executeCustomUpdate(K evictId, String query, final Object... params) {
    	
    	return executeCustomUpdate(evictId, query, new Function<SQLQuery,Query>(){

			@Override
			public Query apply(SQLQuery q) {
				
	            for (int i = 0; i < params.length; i++)
	                q.setParameter(i, params[i]);

	            return q;
			}});    	
    }

    @Override
    public int executeCustomUpdate(K evictId, String query, Function<SQLQuery, Query> bindFunction) {
        StatelessSession ss = null;
        try {
            SQLQuery q;
            try {
            	final Session session = getCurrentSession();
                q = session.createSQLQuery(query);
                
                if (log.isDebugEnabled())
                	log.debug("executeCustomUpdate tx={}: {}#{}", session.getTransaction().getLocalStatus(), entityClass.getSimpleName(), query);                                

            } catch (HibernateException he) {
                propogateIfAbsentMessage(he, NO_SESSION_FOR_THREAD);
                ss = getStatelessSession();
                q = ss.createSQLQuery(query);
                
                if (log.isDebugEnabled())
                	log.debug("executeCustomUpdate stateless: {}#{}", entityClass.getSimpleName(), query);                                
                
            }
            
            bindFunction.apply(q);
            
            try{
            	return q.executeUpdate();
            }finally{
            	if (evictId !=null)
            		sessionFactory.getCache().evictEntity(entityClass, evictId);
            }
        } finally {
            if (ss != null)
                ss.close();
        }
    }

    private void propogateIfAbsentMessage(Throwable e, String message) {
        if (!e.getMessage().equals(message))
            throw Throwables.propagate(e);
    }
    
    @Override
    public Map<K, V> findByIdsAsMap(Collection<K> id) {
        final Map<K, V> ret = new HashMap<K, V>();
        final Collection<V> rs = findByIds(id);
        for (V r : rs) {
            ret.put((K)r.getPk(), r);
        }
        for (K k : id)
            if (!ret.containsKey(k))
                ret.put(k, null);
        return ret;
    }

	@Override
	public V findFirstByCriteria(Criterion criterion, Order order) {
		final List<V> ret = findByCriteria(criterion, order);
		return ret == null || ret.isEmpty() ? null : ret.get(0);
	}

	@Override
	public ListenableFuture<List<V>> findByCriteriaAsync(final Criterion criterion, final Order order) {

		return listeningExecutorService.submit(new Callable<List<V>>(){

			@Override
			public List<V> call() throws Exception {
				return findByCriteria(criterion, order);
			}});
		
	}

	@Override
	public ListenableFuture<V> findFirstByCriteriaAsync(final Criterion criterion, final Order order) {
		return listeningExecutorService.submit(new Callable<V>(){

			@Override
			public V call() throws Exception {
				return findFirstByCriteria(criterion, order);
			}});
	}

    @Override
	public AbstractDao<K,V> with(final SessionFactory sessionFactory){
        return new AbstractDaoImpl<>(sessionFactory, this.entityClass, this.listeningExecutorService);
    }
    
    @Override
    public void evict(K id){
    	this.sessionFactory.getCache().evictEntity(entityClass, id);
    }

}