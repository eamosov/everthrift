package com.knockchat.node.model;

import gnu.trove.map.hash.THashMap;
import gnu.trove.procedure.TObjectProcedure;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Element;
import net.sf.ehcache.loader.CacheLoader;

import org.springframework.beans.factory.annotation.Autowired;

import com.google.common.base.Function;
import com.knockchat.hibernate.dao.DaoEntityIF;

public abstract class AbstractCachedModelFactory<K,V,A, PK extends Serializable,ENTITY extends DaoEntityIF<ENTITY>> extends AbstractLoadableModelFactory<K,V,A, PK, ENTITY> implements MultiLoader<K,V>, InvalidatedIF<K>{
	
	@Autowired
	protected CacheManager cm;
	protected Cache cache;
	
	protected final String cacheName;	
	
	private CacheLoader cacheLoader;

	public AbstractCachedModelFactory(String cacheName){
		super(null);
		this.cacheName = cacheName;
	}

	public AbstractCachedModelFactory(String cacheName, Class<ENTITY> entityClass){
		super(entityClass);
		this.cacheName = cacheName;
	}

	private class IndexInfo<V2>{

		final String name;
		final Cache cache;
		final Function<V, List<Object>> itemToCache;
		final Function<Object[], Object> argsToCache;
		final Function<Object[], List<K>> loader;
		final Function<Object[], List<V2>> loader2; 
		
		public IndexInfo(String name, Cache cache,
				Function<V, List<Object>> itemToCache,
				Function<Object[], Object> argsToCache, Function<Object[], List<K>> loader, Function<Object[], List<V2>> loader2) {
			super();
			this.name = name;
			this.cache = cache;
			this.itemToCache = itemToCache;
			this.argsToCache = argsToCache;
			this.loader = loader;
			this.loader2 = loader2;
		}				
	}
	
	private THashMap<String, IndexInfo<?>> indexes = new THashMap<String, IndexInfo<?>>();
	
	private class IndexInvalidator implements TObjectProcedure<IndexInfo<?>>{
		
		private final V v;
		
		public IndexInvalidator(V v){
			this.v = v;
		}

		@Override
		public boolean execute(IndexInfo<?> object) {
			for (Object i: object.itemToCache.apply(v)){
				object.cache.remove(i);
			}
			return true;
		}
		
	}
	
	private TObjectProcedure<IndexInfo> invalidateIndex = new TObjectProcedure<IndexInfo>(){

		@Override
		public boolean execute(IndexInfo object) {
			// TODO Auto-generated method stub
			return false;
		}
		
	};
	
	protected abstract CacheLoader getCacheLoader();
		
	@Override
	public void afterPropertiesSet() throws Exception {
		super.afterPropertiesSet();
		cacheLoader = getCacheLoader();
		
		if (cacheName !=null){
			cache = cm.getCache(cacheName);
			if (cache == null)
				throw new RuntimeException("Cache with name '" + cacheName + "' not found");
			
			cache.registerCacheLoader(cacheLoader);
		}		
	}
	
	/**
	 * Найти значение по ключю, но не кэшировать в случае загрузки из базы
	 * @param id
	 * @return
	 */
	public V findByIdNoCache(K id){
		V v;
		
		if (cache == null){
			v = (V)cacheLoader.load(id);
		}else{
			Element e  = cache.get(id);				
			if (e!=null){
				v = (V)e.getObjectValue();
			}else{
				v = (V)cacheLoader.load(id);
			}
		}
		return v;
	}

	@Override
	public V findById(K id){
		if (cache == null)
			return (V)cacheLoader.load(id);
		
		final Element e = cache.getWithLoader(id, null, null);
		if (e == null || e.getObjectValue() == null)
			return null;
		
		return (V)e.getObjectValue();
	}
	
	@Override
	public Map<K, V> findByIds(Collection<K> ids){
		
		if (cache == null)
			return (Map<K, V>)cacheLoader.loadAll(ids);
		
		return cache.getAllWithLoader(ids, null);
	}


	/**
	 * Инвалидация индексов будет успешна только в том случае, если индексируемые поля не изменялись
	 */
	@Override
	public void invalidate(K id){
		
		if (cache!=null)
			cache.remove(id);
	}
	
	/**
	 * Использовать этот метод нужно с большой осторожностью, т.к. если далее объект, положенный в кэш, будет изменен (н-р в результате lazyLoad),
	 * то такие изменения проникнут и в кеш. Хорошим решением является всегда передавать в этот метод свежую копию объекта
	 * 
	 * По аналогичным причинам нужно быть уверенным, что передается чистый объект, не испорченный каким-нибудь load*** 
	 * @param e
	 */
	public void refresh(ENTITY e){
		if (cache!=null){
			final Element el = new Element(e.getPk(), e);
			cache.put(el);
		}
	}
	
	public void onInsert(V v){
		indexes.forEachValue(new IndexInvalidator(v));
	}
	
	public void onDelete(V v){
		indexes.forEachValue(new IndexInvalidator(v));
	}

	public void onUpdate(V v){
		indexes.forEachValue(new IndexInvalidator(v));
	}
	
	public void onInvalidate(K id){
		if (indexes.size() !=0){
			final V v = findByIdNoCache(id);
			
			if (v!=null){
				indexes.forEachValue(new IndexInvalidator(v));
			}
		}		
	}

    public void invalidate(Collection<K> ids) {
        if (cache!=null)
            cache.removeAll(ids);
    }

    /**
	 * Создает шаблон для выборки сущностей не по первичному ключу
	 * @param name - название шаблона
	 * @param cache - кэш
	 * @param itemToCache - метод для конвертации сущности в ключи кэша. Используется для инвалидации кэша
	 * @param argsToCache - метод для конвертации аргументов к ключ кэша. Используется для кеширования результатов loader
	 * @param loader - загрузчик, возвращающий идешники сущностей
	 */
	public void addIndex(String name, Cache cache, Function<V, List<Object>> itemToCache, Function<Object[], Object> argsToCache, Function<Object[], List<K>> loader){
		indexes.put(name, new IndexInfo(name, cache, itemToCache, argsToCache, loader, null));
	}

	/**
	 * Отличается от предыдущего тем, что loader грузит сущности любого типа, которые сразу же кэшируются и возвращаются
	 * 
	 * @param name
	 * @param cache
	 * @param itemToCache
	 * @param argsToCache
	 * @param loader
	 */
	public <V2> void addIndex2(String name, Cache cache, Function<V, List<Object>> itemToCache, Function<Object[], Object> argsToCache, Function<Object[], List<V2>> loader){
		indexes.put(name, new IndexInfo(name, cache, itemToCache, argsToCache, null, loader));
	}
	
	public void invalidateByArgs(String indexName, Object[] args){
		final IndexInfo ii = indexes.get(indexName);
		if (ii==null)
			throw new RuntimeException("Index '" + indexName + "' not found");

		final Object cacheKey = ii.argsToCache.apply(args);
		ii.cache.remove(cacheKey);
	}
	
	/**
	 * 
	 * @param indexName
	 * @param args
	 * @return  Нельзя параметризовать, т.к. индекс, созданный через loader2 может вернуть сущность любого типа
	 */
	public List findByArgs(String indexName, Object[] args){
		final IndexInfo ii = indexes.get(indexName);
		if (ii==null)
			throw new RuntimeException("Index '" + indexName + "' not found");

		if (ii.loader2 !=null)
			return findByArgs(ii, args);
		else		
			return this.findByIdsInOrder(findByArgs(ii, args));
	}

	private List findByArgs(IndexInfo<?> ii, Object[] args){
				
		final Object cacheKey = ii.argsToCache.apply(args);
		
		final Element e = ii.cache.get(cacheKey);
		if (e!=null){
			return (List)e.getObjectValue();
		}else{
			final List loaded = ii.loader2 !=null ? ii.loader2.apply(args) : ii.loader.apply(args);
			ii.cache.put(new Element(cacheKey, loaded));
			return loaded;
		}
	}

	public List<K> findIdsByArgs(String indexName, Object[] args){
		
		final IndexInfo<?> ii = indexes.get(indexName);
		if (ii==null)
			throw new RuntimeException("Index '" + indexName + "' not found");
		
		final Object cacheKey = ii.argsToCache.apply(args);
		
		final Element e = ii.cache.get(cacheKey);
		if (e!=null){
			return (List)e.getObjectValue();
		}else{
			final List<K> loaded = ii.loader.apply(args);
			ii.cache.put(new Element(cacheKey, loaded));
			return loaded;
		}
	}

	
	public Cache getCache(){
		return cache;
	}
}
