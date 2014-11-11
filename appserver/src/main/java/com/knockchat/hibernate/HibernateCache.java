package com.knockchat.hibernate;

import java.util.Properties;

import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Ehcache;

import org.hibernate.cache.CacheException;
import org.hibernate.cache.ehcache.internal.nonstop.NonstopAccessStrategyFactory;
import org.hibernate.cache.ehcache.internal.regions.EhcacheCollectionRegion;
import org.hibernate.cache.ehcache.internal.regions.EhcacheEntityRegion;
import org.hibernate.cache.ehcache.internal.regions.EhcacheNaturalIdRegion;
import org.hibernate.cache.ehcache.internal.regions.EhcacheQueryResultsRegion;
import org.hibernate.cache.ehcache.internal.regions.EhcacheTimestampsRegion;
import org.hibernate.cache.ehcache.internal.strategy.EhcacheAccessStrategyFactory;
import org.hibernate.cache.ehcache.internal.strategy.EhcacheAccessStrategyFactoryImpl;
import org.hibernate.cache.ehcache.internal.util.HibernateUtil;
import org.hibernate.cache.spi.CacheDataDescription;
import org.hibernate.cache.spi.CollectionRegion;
import org.hibernate.cache.spi.EntityRegion;
import org.hibernate.cache.spi.NaturalIdRegion;
import org.hibernate.cache.spi.QueryResultsRegion;
import org.hibernate.cache.spi.RegionFactory;
import org.hibernate.cache.spi.TimestampsRegion;
import org.hibernate.cache.spi.access.AccessType;
import org.hibernate.cfg.Settings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.knockchat.appserver.AppserverApplication;

public class HibernateCache implements RegionFactory {

	private final static Logger log = LoggerFactory.getLogger(HibernateCache.class);

    /**
     * Settings object for the Hibernate persistence unit.
     */
    private Settings settings;

    /**
     * {@link EhcacheAccessStrategyFactory} for creating various access strategies
     */
    protected final EhcacheAccessStrategyFactory accessStrategyFactory = new NonstopAccessStrategyFactory( new EhcacheAccessStrategyFactoryImpl() );
    
    public HibernateCache(Properties prop) {
        super();
    }
    
    /**
     * {@inheritDoc}
     */
    public void start(Settings settings, Properties properties) throws CacheException {
        this.settings = settings;
    }

    /**
     * {@inheritDoc}
     */
    public void stop() {
    	
    }    
	
    public boolean isMinimalPutsEnabledByDefault() {
        return true;
    }

    /**
     * {@inheritDoc}
     */
    public long nextTimestamp() {
        return net.sf.ehcache.util.Timestamper.next();
    }

    /**
     * {@inheritDoc}
     */
    public EntityRegion buildEntityRegion(String regionName, Properties properties, CacheDataDescription metadata)
            throws CacheException {
        return new EhcacheEntityRegion( accessStrategyFactory, getCache( regionName ), settings, metadata, properties );
    }
    
    @Override
    public NaturalIdRegion buildNaturalIdRegion(String regionName, Properties properties, CacheDataDescription metadata) throws CacheException {
        return new EhcacheNaturalIdRegion( accessStrategyFactory, getCache( regionName ), settings, metadata, properties );
    }

    /**
     * {@inheritDoc}
     */
    public CollectionRegion buildCollectionRegion(String regionName, Properties properties, CacheDataDescription metadata) throws CacheException {
        return new EhcacheCollectionRegion(
                accessStrategyFactory,
                getCache( regionName ),
                settings,
                metadata,
                properties
        );
    }

    /**
     * {@inheritDoc}
     */
    public QueryResultsRegion buildQueryResultsRegion(String regionName, Properties properties) throws CacheException {
        return new EhcacheQueryResultsRegion( accessStrategyFactory, getCache( regionName ), properties );
    }

    /**
     * {@inheritDoc}
     */
    public TimestampsRegion buildTimestampsRegion(String regionName, Properties properties) throws CacheException {
        return new EhcacheTimestampsRegion( accessStrategyFactory, getCache( regionName ), properties );
    }

    private Ehcache getCache(String name) throws CacheException {
        try {
        	
        	final CacheManager manager = AppserverApplication.INSTANCE.context.getBean(CacheManager.class);
        	
            Ehcache cache = manager.getEhcache( name );
            if ( cache == null ) {
                log.info("unableToFindEhCacheConfiguration: {}", name );
                manager.addCache( name );
                cache = manager.getEhcache( name );
                log.debug( "started EHCache region: {}", name );
            }
            HibernateUtil.validateEhcache( cache );
            return cache;
        }
        catch ( net.sf.ehcache.CacheException e ) {
            throw new CacheException( e );
        }

    }

    /**
     * Default access-type used when the configured using JPA 2.0 config.  JPA 2.0 allows <code>@Cacheable(true)</code> to be attached to an
     * entity without any access type or usage qualification.
     * <p/>
     * We are conservative here in specifying {@link AccessType#READ_WRITE} so as to follow the mantra of "do no harm".
     * <p/>
     * This is a Hibernate 3.5 method.
     */
    public AccessType getDefaultAccessType() {
        return AccessType.READ_WRITE;
    }

}
