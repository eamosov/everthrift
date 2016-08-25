package org.everthrift.sql.hibernate;

import java.util.Properties;

import org.everthrift.appserver.AppserverApplication;
import org.hibernate.boot.spi.SessionFactoryOptions;
import org.hibernate.cache.CacheException;
import org.hibernate.cache.ehcache.internal.nonstop.NonstopAccessStrategyFactory;
import org.hibernate.cache.ehcache.internal.regions.EhcacheCollectionRegion;
import org.hibernate.cache.ehcache.internal.regions.EhcacheEntityRegion;
import org.hibernate.cache.ehcache.internal.regions.EhcacheNaturalIdRegion;
import org.hibernate.cache.ehcache.internal.regions.EhcacheQueryResultsRegion;
import org.hibernate.cache.ehcache.internal.regions.EhcacheTimestampsRegion;
import org.hibernate.cache.ehcache.internal.strategy.EhcacheAccessStrategyFactory;
import org.hibernate.cache.ehcache.internal.strategy.EhcacheAccessStrategyFactoryImpl;
import org.hibernate.cache.spi.CacheDataDescription;
import org.hibernate.cache.spi.CollectionRegion;
import org.hibernate.cache.spi.EntityRegion;
import org.hibernate.cache.spi.NaturalIdRegion;
import org.hibernate.cache.spi.QueryResultsRegion;
import org.hibernate.cache.spi.RegionFactory;
import org.hibernate.cache.spi.TimestampsRegion;
import org.hibernate.cache.spi.access.AccessType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Ehcache;

public class HibernateCache implements RegionFactory {

    private final static Logger log = LoggerFactory.getLogger(HibernateCache.class);

    /**
     * Settings object for the Hibernate persistence unit.
     */
    private SessionFactoryOptions settings;

    /**
     * {@link EhcacheAccessStrategyFactory} for creating various access
     * strategies
     */
    protected final EhcacheAccessStrategyFactory accessStrategyFactory = new NonstopAccessStrategyFactory(new EhcacheAccessStrategyFactoryImpl());

    public HibernateCache(Properties prop) {
        super();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void start(SessionFactoryOptions settings, Properties properties) throws CacheException {
        this.settings = settings;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void stop() {

    }

    @Override
    public boolean isMinimalPutsEnabledByDefault() {
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long nextTimestamp() {
        return net.sf.ehcache.util.Timestamper.next();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public EntityRegion buildEntityRegion(String regionName, Properties properties, CacheDataDescription metadata) throws CacheException {
        return new EhcacheEntityRegion(accessStrategyFactory, getCache(regionName), settings, metadata, properties);
    }

    @Override
    public NaturalIdRegion buildNaturalIdRegion(String regionName, Properties properties,
                                                CacheDataDescription metadata) throws CacheException {
        return new EhcacheNaturalIdRegion(accessStrategyFactory, getCache(regionName), settings, metadata, properties);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CollectionRegion buildCollectionRegion(String regionName, Properties properties,
                                                  CacheDataDescription metadata) throws CacheException {
        return new EhcacheCollectionRegion(accessStrategyFactory, getCache(regionName), settings, metadata, properties);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public QueryResultsRegion buildQueryResultsRegion(String regionName, Properties properties) throws CacheException {
        return new EhcacheQueryResultsRegion(accessStrategyFactory, getCache(regionName), properties);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TimestampsRegion buildTimestampsRegion(String regionName, Properties properties) throws CacheException {
        return new EhcacheTimestampsRegion(accessStrategyFactory, getCache(regionName), properties);
    }

    private Ehcache getCache(String name) throws CacheException {
        try {

            final CacheManager manager = AppserverApplication.INSTANCE.context.getBean(CacheManager.class);

            Ehcache cache = manager.getEhcache(name);
            if (cache == null) {
                log.info("unableToFindEhCacheConfiguration: {}", name);
                manager.addCache(name);
                cache = manager.getEhcache(name);
                log.debug("started EHCache region: {}", name);
            }
            // HibernateUtil.validateEhcache( cache );
            return cache;
        }
        catch (net.sf.ehcache.CacheException e) {
            throw new CacheException(e);
        }

    }

    /**
     * Default access-type used when the configured using JPA 2.0 config. JPA
     * 2.0 allows <code>@Cacheable(true)</code> to be attached to an entity
     * without any access type or usage qualification.
     * <p/>
     * We are conservative here in specifying {@link AccessType#READ_WRITE} so
     * as to follow the mantra of "do no harm".
     * <p/>
     * This is a Hibernate 3.5 method.
     */
    @Override
    public AccessType getDefaultAccessType() {
        return AccessType.READ_WRITE;
    }

}
