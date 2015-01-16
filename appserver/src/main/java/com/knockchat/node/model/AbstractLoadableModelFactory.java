package com.knockchat.node.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.NotImplementedException;
import org.apache.thrift.LoadException;
import org.apache.thrift.TBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.lambdaj.function.argument.Argument;
import ch.lambdaj.function.argument.ArgumentsFactory;
import ch.lambdaj.function.closure.Closure2;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.knockchat.appserver.model.LazyLoaderHelper;
import com.knockchat.hibernate.dao.DaoEntityIF;
import com.knockchat.utils.Function2;

public abstract class AbstractLoadableModelFactory<K, V, A, PK extends Serializable,ENTITY extends DaoEntityIF<ENTITY>> extends AbstractModelFactory<PK, ENTITY> implements MultiLoader<K, V> {

    private final Logger log = LoggerFactory.getLogger(this.getClass());
    
    public abstract V findById(K id);

    @Override
    public abstract Map<K, V> findByIds(Collection<K> ids);
    
    protected final LazyLoaderHelper<XAwareIF<K, V>, V> lazyLoader = new LazyLoaderHelper<XAwareIF<K, V>, V>(){

		@Override
		protected int loadImpl(Iterable<? extends XAwareIF<K, V>> input) {
			return _load(input);
		}

		@Override
		protected void loadImpl(XAwareIF<K, V> input) {
			input.set(findById((K) input.getId()));
		}
    	
		@Override
	    protected boolean beforeLoad(XAwareIF<K, V> key){
	        if (!key.isSetId())
	            return false;
	        
	        return true;
	    }
    };
    
    protected final LazyLoaderHelper<XAwareIF<List<K>, List<V>>, List<V>> lazyListLoader = new LazyLoaderHelper<XAwareIF<List<K>, List<V>>, List<V>>(){

		@Override
		protected int loadImpl(Iterable<? extends XAwareIF<List<K>, List<V>>> input) {
			return _loadList(input);
		}

		@Override
		protected void loadImpl(XAwareIF<List<K>, List<V>> input) {
			input.set(findByIdsInOrder(input.getId()));
		}
    	
		@Override
	    protected boolean beforeLoad(XAwareIF<List<K>, List<V>> key){
	        if (!key.isSetId())
	            return false;
	        
	        return true;
	    }
    };
    
    public AbstractLoadableModelFactory(Class<ENTITY> entityClass, Storage storage) {
    	super(entityClass, storage);
    }

    protected XAwareIF<K, V> getAwareAdapter(final A m) {
        throw new NotImplementedException();
    }
    
    public void lazyLoad(XAwareIF<K, V> m) {
        this._lazyLoad(m);
    }    

    public LoadException lazyLoadThrow(XAwareIF<K, V> m) {
    	lazyLoad(m);
    	return new LoadException();
    }    

    public void lazyLoad(final A m) {
        this._lazyLoad(getAwareAdapter(m));
    }

    public LoadException lazyLoadThrow(final A m) {
        lazyLoad(m);
        return new LoadException();
    }

    public LoadException lazyLoadThrow(final TBase m, final String fieldName) {
        lazyLoad(m, fieldName);
        return new LoadException();
    }
    
    public LoadException lazyLoadThrow(final TBase m, final String isSetIdMethodName, final String getIdMethodName, final String setMethodName) {
    	lazyLoad(m, isSetIdMethodName, getIdMethodName, setMethodName);
    	return new LoadException();
    }

    public void lazyLoad(final TBase m, final String isSetIdMethodName, final String getIdMethodName, final String setMethodName) {
        this._lazyLoad(AwareAdapterFactory.<K, V>getAwareAdapter(m, isSetIdMethodName, getIdMethodName, setMethodName));
    }

    public void lazyLoad(final TBase m, final String fieldName) {
       	this._lazyLoad(AwareAdapterFactory.<K, V>getAwareAdapter(m, fieldName));
    }
    
    private final Function<A, XAwareIF<K, V>> getAdapterFunction = new   Function<A, XAwareIF<K, V>>(){

		@Override
		public XAwareIF<K, V> apply(A input) {
			return getAwareAdapter(input);
		}};

    public void load(Iterable<? extends A> s) {
    	
    	if (s instanceof Collection && ((Collection)s).size() == 0)
    		return;
    	    	
        this.load(s, getAdapterFunction);
    }

    public <I> void load(Iterable<? extends I> s, Function<I, XAwareIF<K, V>> adapter) {
    	
    	if (s instanceof Collection && ((Collection)s).size() == 0)
    		return;
    	
        this._load(Iterables.transform(s, adapter));
    }
    
    protected void _lazyLoad(XAwareIF<K, V> m) {
        lazyLoader.load(m);
    }
    
    public void lazyListLoad(XAwareIF<List<K>, List<V>> m) {
        lazyListLoader.load(m);
    }
    
    public LoadException lazyListLoadThrow(XAwareIF<List<K>, List<V>> m) {
    	lazyListLoad(m);
    	return new LoadException();
    }
    
    protected int _loadList(Iterable<? extends XAwareIF<List<K>, List<V>>> s) {

        return joinByIds(s, new Function<XAwareIF<List<K>, List<V>>, List<K>>() {

            @Override
            public List<K> apply(XAwareIF<List<K>, List<V>> input) {
                return input.isSetId() ? input.getId() : null;
            }
        }, new Function2<XAwareIF<List<K>, List<V>>, List<V>, Void>() {

            @Override
            public Void apply(XAwareIF<List<K>, List<V>> input1, List<V> input2) {
                input1.set(input2);
                return null;
            }
        }, new MultiLoader<List<K>, List<V>>(){

			@Override
			public Map<List<K>, List<V>> findByIds(Collection<List<K>> ids) {
				return findByCollectionIds(ids);
			}
        });

    }

    protected int _load(Iterable<? extends XAwareIF<K, V>> s) {

        if (log.isDebugEnabled()) {
            int i = 0;
            for (Object j : s)
                i++;
            log.debug("loading {} entities", i);
        }

        return joinByIds(s, new Function<XAwareIF<K, V>, K>() {

            @Override
            public K apply(XAwareIF<K, V> input) {
                return input.isSetId() ? input.getId() : null;
            }
        }, new Function2<XAwareIF<K, V>, V, Void>() {

            @Override
            public Void apply(XAwareIF<K, V> input1, V input2) {
                input1.set(input2);
                return null;
            }
        });
    }
        
    public Map<List<K>, List<V>> findByCollectionIds(Collection<List<K>> listCollection) {
    	
    	if (listCollection.size() == 0)
    		return Collections.emptyMap();
    	
        final List<K> totalIds = new ArrayList<>();
        for (Collection<K> ids : listCollection) {
            totalIds.addAll(ids);
        }
        
        final Map<K, V> intermediateResult = findByIds(totalIds);
        final Map<List<K>, List<V>> result = new HashMap<>();
        
        for (List<K> ids : listCollection) {
            List<V> places = new ArrayList<>();
            for (K id : ids)
                places.add(intermediateResult.get(id));
            result.put(ids, places);
        }
        return result;
    }    

    public List<V> findByIdsInOrder(Collection<K> ids) {
    	
    	if (ids.size() == 0)
    		return Collections.emptyList();
    	
        final List<V> ret = new ArrayList<V>();
        final Map<K, V> loaded = findByIds(ids);
        for (K id : ids) {
            final V v = loaded.get(id);
            if (v != null)
                ret.add(v);
        }
        return ret;
    }

    /**
     * @param s
     * @param loadedUserId Lambda.on
     * @param setUserId
     */
    public <T> int joinByIds(Iterable<? extends T> s, K getEntityId, Closure2<T, V> setEntity) {
        return joinByIds(s, getEntityId, setEntity, this);
    }

    public <T> int joinByIds(Iterable<? extends T> s, Function<T, K> getEntityId, Function2<T, V, Void> setEntity) {
        return joinByIds(s, getEntityId, setEntity, this);
    }

    /**
     * @param s
     * @param getEntityId Lambda.on
     * @param setEntity
     */
    public static <K, U, T> int joinByIds(Iterable<? extends T> s, K getEntityId, Closure2<T, U> setEntity, MultiLoader<K, U> loader) {

        final Argument<K> actualArgument = ArgumentsFactory.actualArgument(getEntityId);

        final List<K> ids = new ArrayList<K>();
        for (T i : s) {
            ids.add(actualArgument.evaluate(i));
        }

        final Map<K, U> loaded = loader.findByIds(ids);

        int k = 0;
        for (T i : s) {
            U l = loaded.get(actualArgument.evaluate(i));
            if (l != null)
                k++;
            setEntity.apply(i, l);
        }

        return k;
    }

    /**
     *  K - тип ключа привязываемого объекта
     *  U - тип привязываемого объекта
     *  T - тип объекта, к которому происходит привязка
     * 
     * @param s - список объектов, к которым добавить связь
     * @param getEntityId - функция получения ключа(K) связанного объекта
     * @param setEntity - функция присвоения связи
     * @param loader - загрузчик объектов для связи(U) по их ключам (K)
     */
    public static <K, U, T> int joinByIds(Iterable<? extends T> s, Function<T, K> getEntityId, Function2<T, U, Void> setEntity, MultiLoader<K, U> loader) {
    	
    	if (s instanceof Collection && ((Collection)s).size() == 0)
    		return 0;

        final Set<K> ids = new HashSet<K>();
        for (T i : s) {
            final K id = getEntityId.apply(i);
            if (id != null)
                ids.add(id);
        }

        final Map<K, U> loaded = loader.findByIds(ids);
        int k = 0;
        for (T i : s) {
            final K id = getEntityId.apply(i);
            if (id == null)
                continue;

            final U u = loaded.get(id);
            if (u != null) {
                k++;
            }
            setEntity.apply(i, u);
        }
        return k;
    }

}
