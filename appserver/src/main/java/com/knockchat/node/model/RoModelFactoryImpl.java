package com.knockchat.node.model;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.RandomAccess;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Predicates;
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.knockchat.appserver.model.lazy.AbstractLazyLoader;
import com.knockchat.appserver.model.lazy.Registry;
import com.knockchat.utils.Function2;


public abstract class RoModelFactoryImpl<PK, ENTITY>  implements RoModelFactoryIF<PK, ENTITY>{
	
	public RoModelFactoryImpl() {
		
	}

	private static interface MultiLoader<K,V> {

		public Map<K, V> findByIds(Collection<K> ids);
	}
	
	protected final Logger log = LoggerFactory.getLogger(this.getClass());
	
    
    @Override
    final public Collection<ENTITY> findEntityById(Collection<PK> ids){
    	
    	if (CollectionUtils.isEmpty(ids))
    		return Collections.emptyList();
    	
    	return Collections2.filter(findEntityByIdAsMap(ids).values(), Predicates.notNull());
    }        
	
    @Override
    final public List<ENTITY> findEntityByIdsInOrder(Collection<PK> ids) {
    	
    	if (CollectionUtils.isEmpty(ids))
    		return Collections.emptyList();
    	
        final List<ENTITY> ret = Lists.newArrayListWithExpectedSize(ids.size());
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
    	
    	if (CollectionUtils.isEmpty(listCollection))
    		return Collections.emptyMap();
    	
        final List<PK> totalIds = Lists.newArrayListWithCapacity(listCollection.size() * 2);
        for (Collection<PK> ids : listCollection) {
            totalIds.addAll(ids);
        }
        
        final Map<PK, ENTITY> intermediateResult = findEntityByIdAsMap(totalIds);
        final Map<List<PK>, List<ENTITY>> result = Maps.newHashMapWithExpectedSize(listCollection.size());
        
        for (List<PK> ids : listCollection) {
            List<ENTITY> places = Lists.newArrayListWithCapacity(ids.size());
            for (PK id : ids)
                places.add(intermediateResult.get(id));
            result.put(ids, places);
        }
        return result;
    }    
     
    protected final AbstractLazyLoader<XAwareIF<PK, ENTITY>> lazyLoader = new AbstractLazyLoader<XAwareIF<PK, ENTITY>>(){
    	
		@Override
	    protected boolean beforeLoad(XAwareIF<PK, ENTITY> key){
	        if (!key.isSetId())
	            return false;
	        
	        return true;
	    }

		@Override
		protected int loadImpl(List<XAwareIF<PK, ENTITY>> entities) {
			return _load(entities);
		}
    };
    
    protected final AbstractLazyLoader<XAwareIF<List<PK>, List<ENTITY>>> lazyListLoader = new AbstractLazyLoader<XAwareIF<List<PK>, List<ENTITY>>>(){

		@Override
	    protected boolean beforeLoad(XAwareIF<List<PK>, List<ENTITY>> key){
	        if (!key.isSetId())
	            return false;
	        
	        return true;
	    }

		@Override
		protected int loadImpl(List<XAwareIF<List<PK>, List<ENTITY>>> entities) {
			return _loadList(entities);
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
		
        
    public void lazyLoad(Registry r, XAwareIF<PK, ENTITY> m) {
    	lazyLoader.load(r, m);
    }    

    
    public void lazyListLoad(Registry r, XAwareIF<List<PK>, List<ENTITY>> m) {
        lazyListLoader.load(r, m);
    }
    
    private Function<XAwareIF<List<PK>, List<ENTITY>>, List<PK>> _listGetEntityId = new Function<XAwareIF<List<PK>, List<ENTITY>>, List<PK>>() {

        @Override
        public List<PK> apply(XAwareIF<List<PK>, List<ENTITY>> input) {
            return input.isSetId() ? input.getId() : null;
        }
    };
    
    private Function2<XAwareIF<List<PK>, List<ENTITY>>, List<ENTITY>, Void> _listSetEntity = new Function2<XAwareIF<List<PK>, List<ENTITY>>, List<ENTITY>, Void>() {

        @Override
        public Void apply(XAwareIF<List<PK>, List<ENTITY>> input1, List<ENTITY> input2) {
            input1.set(input2);
            return null;
        }
    };
    
    protected int _loadList(Iterable<? extends XAwareIF<List<PK>, List<ENTITY>>> s) {

        return joinByIds(s, _listGetEntityId, _listSetEntity, multiListLoader);

    }
    
    private Function<XAwareIF<PK, ENTITY>, PK> _getEntityId = new Function<XAwareIF<PK, ENTITY>, PK>() {

        @Override
        public PK apply(XAwareIF<PK, ENTITY> input) {
            return input.isSetId() ? input.getId() : null;
        }
    };
    
    private Function2<XAwareIF<PK, ENTITY>, ENTITY, Void> _setEntity = new Function2<XAwareIF<PK, ENTITY>, ENTITY, Void>() {

        @Override
        public Void apply(XAwareIF<PK, ENTITY> input1, ENTITY input2) {
            input1.set(input2);
            return null;
        }
    };

    protected int _load(Iterable<? extends XAwareIF<PK, ENTITY>> s) {

        if (log.isDebugEnabled()) {
            int i = 0;
            for (Object j : s)
                i++;
            log.debug("loading {} entities", i);
        }

        return joinByIds(s, _getEntityId, _setEntity);
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
    public static <K, U, T> int joinByIds(final Iterable<? extends T> s, Function<T, K> getEntityId, Function2<T, U, Void> setEntity, MultiLoader<K, U> loader) {
    	
    	if (s instanceof Collection && ((Collection)s).size() == 0)
    		return 0;

        final Set<K> ids = new HashSet<K>();
        
        if (s instanceof RandomAccess){
        	
        	final List<? extends T> _s = (List)s;
        	
        	for (int i=0; i<_s.size() ; i++){
                final K id = getEntityId.apply(_s.get(i));
                if (id != null)
                    ids.add(id);        		
        	}
        	
        }else{
            for (T i : s) {
                final K id = getEntityId.apply(i);
                if (id != null)
                    ids.add(id);
            }        	
        }        

        final Map<K, U> loaded = loader.findByIds(ids);
        int k = 0;
        if (s instanceof RandomAccess){
        	final List<? extends T> _s = (List)s;
        	for (int j=0; j<_s.size(); j++){
        		final T i = _s.get(j);
                final K id = getEntityId.apply(i);
                if (id == null)
                    continue;

                final U u = loaded.get(id);
                if (u != null) {
                    k++;
                }
                setEntity.apply(i, u);        		
        	}
        }else{
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
        }
        return k;
    }
    	
}
