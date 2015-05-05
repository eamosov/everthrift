package com.knockchat.node.model;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.NotImplementedException;
import org.apache.thrift.LoadException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Predicates;
import com.google.common.collect.Collections2;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.knockchat.appserver.model.LazyLoaderHelper;
import com.knockchat.utils.Function2;


public abstract class RoModelFactoryImpl<PK, ENTITY, A>  implements RoModelFactoryIF<PK, ENTITY>{

	public RoModelFactoryImpl() {
		
	}

	private static interface MultiLoader<K,V> {

		public Map<K, V> findByIds(Collection<K> ids);
	}
	
	protected final Logger log = LoggerFactory.getLogger(this.getClass());
	
    
    @Override
    final public Collection<ENTITY> findEntityById(Collection<PK> ids){
    	return Collections2.filter(findEntityByIdAsMap(ids).values(), Predicates.notNull());
    }        
	
    @Override
    final public List<ENTITY> findEntityByIdsInOrder(Collection<PK> ids) {
    	
    	if (CollectionUtils.isEmpty(ids))
    		return Collections.emptyList();
    	
        final List<ENTITY> ret = new ArrayList<ENTITY>();
        final Map<PK, ENTITY> loaded = findEntityByIdAsMap(ids);
        for (PK id : ids) {
            final ENTITY v = loaded.get(id);
            if (v != null)
                ret.add(v);
        }
        return ret;
    }
    
    @Override
	public Map<List<PK>, List<ENTITY>> findEntityByCollectionIds(Collection<List<PK>> listCollection) {
    	
    	if (listCollection.size() == 0)
    		return Collections.emptyMap();
    	
        final List<PK> totalIds = new ArrayList<>();
        for (Collection<PK> ids : listCollection) {
            totalIds.addAll(ids);
        }
        
        final Map<PK, ENTITY> intermediateResult = findEntityByIdAsMap(totalIds);
        final Map<List<PK>, List<ENTITY>> result = Maps.newHashMap();
        
        for (List<PK> ids : listCollection) {
            List<ENTITY> places = new ArrayList<>();
            for (PK id : ids)
                places.add(intermediateResult.get(id));
            result.put(ids, places);
        }
        return result;
    }    
     
    protected final LazyLoaderHelper<XAwareIF<PK, ENTITY>, ENTITY> lazyLoader = new LazyLoaderHelper<XAwareIF<PK, ENTITY>, ENTITY>(){

		@Override
		protected int loadImpl(Iterable<? extends XAwareIF<PK, ENTITY>> input) {
			return _load(input);
		}

		@Override
		protected void loadImpl(XAwareIF<PK, ENTITY> input) {
			input.set(findEntityById((PK) input.getId()));
		}
    	
		@Override
	    protected boolean beforeLoad(XAwareIF<PK, ENTITY> key){
	        if (!key.isSetId())
	            return false;
	        
	        return true;
	    }
    };
    
    protected final LazyLoaderHelper<XAwareIF<List<PK>, List<ENTITY>>, List<ENTITY>> lazyListLoader = new LazyLoaderHelper<XAwareIF<List<PK>, List<ENTITY>>, List<ENTITY>>(){

		@Override
		protected int loadImpl(Iterable<? extends XAwareIF<List<PK>, List<ENTITY>>> input) {
			return _loadList(input);
		}

		@Override
		protected void loadImpl(XAwareIF<List<PK>, List<ENTITY>> input) {
			input.set(findEntityByIdsInOrder(input.getId()));
		}
    	
		@Override
	    protected boolean beforeLoad(XAwareIF<List<PK>, List<ENTITY>> key){
	        if (!key.isSetId())
	            return false;
	        
	        return true;
	    }
    };
    
    private final MultiLoader<PK, ENTITY> multiLoader = new MultiLoader<PK, ENTITY>(){

		@Override
		public Map<PK, ENTITY> findByIds(Collection<PK> ids) {				
			return findEntityByIdAsMap(ids);
		}};
		
    private final MultiLoader<List<PK>, List<ENTITY>> multiListLoader = new MultiLoader<List<PK>, List<ENTITY>>(){

		@Override
		public Map<List<PK>, List<ENTITY>> findByIds(Collection<List<PK>> ids) {				
			return RoModelFactoryImpl.this.findEntityByCollectionIds(ids);
		}};
		
        
    public void lazyLoad(XAwareIF<PK, ENTITY> m) {
        this._lazyLoad(m);
    }    

    public LoadException lazyLoadThrow(XAwareIF<PK, ENTITY> m) {
    	lazyLoad(m);
    	return new LoadException();
    }    

    public <I> void load(Iterable<? extends I> s, Function<I, XAwareIF<PK, ENTITY>> adapter) {
    	
    	if (s instanceof Collection && ((Collection)s).size() == 0)
    		return;
    	
        this._load(Iterables.transform(s, adapter));
    }
    
    protected void _lazyLoad(XAwareIF<PK, ENTITY> m) {
        lazyLoader.load(m);
    }
    
    public void lazyListLoad(XAwareIF<List<PK>, List<ENTITY>> m) {
        lazyListLoader.load(m);
    }
    
    public LoadException lazyListLoadThrow(XAwareIF<List<PK>, List<ENTITY>> m) {
    	lazyListLoad(m);
    	return new LoadException();
    }
    
    protected int _loadList(Iterable<? extends XAwareIF<List<PK>, List<ENTITY>>> s) {

        return joinByIds(s, new Function<XAwareIF<List<PK>, List<ENTITY>>, List<PK>>() {

            @Override
            public List<PK> apply(XAwareIF<List<PK>, List<ENTITY>> input) {
                return input.isSetId() ? input.getId() : null;
            }
        }, new Function2<XAwareIF<List<PK>, List<ENTITY>>, List<ENTITY>, Void>() {

            @Override
            public Void apply(XAwareIF<List<PK>, List<ENTITY>> input1, List<ENTITY> input2) {
                input1.set(input2);
                return null;
            }
        }, multiListLoader);

    }

    protected int _load(Iterable<? extends XAwareIF<PK, ENTITY>> s) {

        if (log.isDebugEnabled()) {
            int i = 0;
            for (Object j : s)
                i++;
            log.debug("loading {} entities", i);
        }

        return joinByIds(s, new Function<XAwareIF<PK, ENTITY>, PK>() {

            @Override
            public PK apply(XAwareIF<PK, ENTITY> input) {
                return input.isSetId() ? input.getId() : null;
            }
        }, new Function2<XAwareIF<PK, ENTITY>, ENTITY, Void>() {

            @Override
            public Void apply(XAwareIF<PK, ENTITY> input1, ENTITY input2) {
                input1.set(input2);
                return null;
            }
        });
    }
        

    public <T> int joinByIds(Iterable<? extends T> s, Function<T, PK> getEntityId, Function2<T, ENTITY, Void> setEntity) {
        return joinByIds(s, getEntityId, setEntity, multiLoader);
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
    
    protected XAwareIF<PK, ENTITY> getAwareAdapter(final A m) {
        throw new NotImplementedException();
    }
    
    public void lazyLoad(final A m) {
        this._lazyLoad(getAwareAdapter(m));
    }

    public LoadException lazyLoadThrow(final A m) {
        lazyLoad(m);
        return new LoadException();
    }
	
}
