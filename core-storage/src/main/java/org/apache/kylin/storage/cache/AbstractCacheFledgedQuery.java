package org.apache.kylin.storage.cache;

import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Element;
import net.sf.ehcache.Status;
import net.sf.ehcache.config.CacheConfiguration;
import net.sf.ehcache.config.Configuration;
import net.sf.ehcache.config.PersistenceConfiguration;
import net.sf.ehcache.store.MemoryStoreEvictionPolicy;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.realization.StreamSQLDigest;
import org.apache.kylin.metadata.tuple.TeeTupleItrListener;
import org.apache.kylin.storage.ICachableStorageQuery;
import org.apache.kylin.storage.IStorageQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 */
public abstract class AbstractCacheFledgedQuery implements IStorageQuery, TeeTupleItrListener {
    private static final Logger logger = LoggerFactory.getLogger(AbstractCacheFledgedQuery.class);
    private static final String storageCacheTemplate = "StorageCache";

    protected static CacheManager CACHE_MANAGER;

    protected ICachableStorageQuery underlyingStorage;
    protected StreamSQLDigest streamSQLDigest;

    public AbstractCacheFledgedQuery(ICachableStorageQuery underlyingStorage) {
        this.underlyingStorage = underlyingStorage;
        this.makeCacheIfNecessary(underlyingStorage.getStorageUUID());
    }

    public static void setCacheManager(CacheManager cacheManager) {
        CACHE_MANAGER = cacheManager;
    }

    /**
     * This method is only useful non-spring injected test cases.
     * When Kylin is normally ran as a spring app CACHE_MANAGER will be injected.
     * and the configuration for cache lies in server/src/main/resources/ehcache.xml
     * 
     * the cache named "StorageCache" acts like a template for each realization to
     * create its own cache.
     */
    private static void initCacheManger() {
        Configuration conf = new Configuration();
        conf.setMaxBytesLocalHeap("128M");
        CACHE_MANAGER = CacheManager.create(conf);

        //a fake template for test cases
        Cache storageCache = new Cache(new CacheConfiguration(storageCacheTemplate, 0).//
                memoryStoreEvictionPolicy(MemoryStoreEvictionPolicy.LRU).//
                eternal(false).//
                timeToIdleSeconds(86400).//
                diskExpiryThreadIntervalSeconds(0).//
                //maxBytesLocalHeap(10, MemoryUnit.MEGABYTES).//
                persistence(new PersistenceConfiguration().strategy(PersistenceConfiguration.Strategy.NONE)));

        CACHE_MANAGER.addCacheIfAbsent(storageCache);
    }

    protected StreamSQLResult getStreamSQLResult(StreamSQLDigest streamSQLDigest) {

        Cache cache = CACHE_MANAGER.getCache(this.underlyingStorage.getStorageUUID());
        Element element = cache.get(streamSQLDigest.hashCode());//TODO: hash code cannot guarantee uniqueness
        if (element != null) {
            return (StreamSQLResult) element.getObjectValue();
        }
        return null;
    }

    protected boolean needSaveCache(long createTime) {
        long storageQueryTime = System.currentTimeMillis() - createTime;
        long durationThreshold = KylinConfig.getInstanceFromEnv().getQueryDurationCacheThreshold();
        //TODO: check scan count necessary?

        if (storageQueryTime < durationThreshold) {
            logger.info("Skip saving storage caching for storage cache because storage query time {} less than {}", storageQueryTime, durationThreshold);
            return false;
        }

        return true;
    }

    private void makeCacheIfNecessary(String storageUUID) {
        if (CACHE_MANAGER == null || (!(CACHE_MANAGER.getStatus().equals(Status.STATUS_ALIVE)))) {
            logger.warn("CACHE_MANAGER is not provided or not alive");
            initCacheManger();
        }

        if (CACHE_MANAGER.getCache(storageUUID) == null) {
            logger.info("Cache for {} initializing...", storageUUID);

            //Create a Cache specifying its configuration.
            CacheConfiguration templateConf = CACHE_MANAGER.getCache(storageCacheTemplate).getCacheConfiguration();

            Cache storageCache = new Cache(new CacheConfiguration(storageUUID, 0).//
                    memoryStoreEvictionPolicy(templateConf.getMemoryStoreEvictionPolicy()).//
                    eternal(templateConf.isEternal()).//
                    timeToIdleSeconds(templateConf.getTimeToIdleSeconds()).//
                    //maxBytesLocalHeap(templateConf.getMaxBytesLocalHeap(), MemoryUnit.BYTES).//using pooled size
                    persistence(templateConf.getPersistenceConfiguration()));

            CACHE_MANAGER.addCacheIfAbsent(storageCache);
        }
    }
}
