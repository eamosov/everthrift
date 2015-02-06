package com.knockchat.node.model;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Element;
import net.sf.ehcache.loader.CacheLoader;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.NotImplementedException;
import org.springframework.beans.factory.annotation.Autowired;

import com.knockchat.hibernate.dao.DaoEntityIF;

public abstract class AbstractCachedModelFactory<K,V,A, PK extends Serializable,ENTITY extends DaoEntityIF<ENTITY>> extends AbstractLoadableModelFactory<K,V,A, PK, ENTITY> implements MultiLoader<K,V>, InvalidatedIF<K>{
	
	@Autowired
	protected CacheManager cm;
	protected Cache cache;
	
	protected final String cacheName;	
	
	private CacheLoader cacheLoader;

	public AbstractCachedModelFactory(String cacheName){
		super(null, null);
		this.cacheName = cacheName;
	}

	public AbstractCachedModelFactory(String cacheName, Class<ENTITY> entityClass, Storage storage){
		super(entityClass, storage);
		this.cacheName = cacheName;
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	protected CacheLoader getCacheLoader(){
		if (storage == Storage.PGSQL)
			return new DaoCacheLoader(cacheName + "Loader", getDao());
		else if (storage == Storage.MONGO)
			return new MongoCacheLoader(cacheName + "Loader", getMongo(), entityClass);
		else
			throw new NotImplementedException();
	}
		
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
			v = fetchById(id);
		}else{
			Element e  = cache.get(id);				
			if (e!=null){
				v = (V)e.getObjectValue();
			}else{
				v = fetchById(id);
			}
		}
		return v;
	}

	/**
	 * Загрузить значение по ключю из базы
	 * @param id
	 * @return
	 */
	public V fetchById(K id){
		return (V)cacheLoader.load(id);
	}
	
	@Override
	public V findById(K id){
		if (cache == null)
			return fetchById(id);
		
		final Element e = cache.getWithLoader(id, null, null);
		if (e == null || e.getObjectValue() == null)
			return null;
		
		return (V)e.getObjectValue();
	}
	
	@Override
	public Map<K, V> findByIds(Collection<K> ids){
		
		if (CollectionUtils.isEmpty(ids))
			return Collections.emptyMap();
		
		if (cache == null)
			return (Map<K, V>)cacheLoader.loadAll(ids);
		
		return cache.getAllWithLoader(ids, null);
	}


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
	
    public void invalidate(Collection<K> ids) {
        if (cache!=null)
            cache.removeAll(ids);
    }
	
	public Cache getCache(){
		return cache;
	}
}
