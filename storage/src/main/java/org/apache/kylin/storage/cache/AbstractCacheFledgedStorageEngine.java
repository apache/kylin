package org.apache.kylin.storage.cache;

import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.config.CacheConfiguration;
import net.sf.ehcache.config.Configuration;
import net.sf.ehcache.config.MemoryUnit;
import net.sf.ehcache.config.PersistenceConfiguration;
import net.sf.ehcache.store.MemoryStoreEvictionPolicy;

import org.apache.kylin.metadata.realization.StreamSQLDigest;
import org.apache.kylin.metadata.tuple.TeeTupleItrListener;
import org.apache.kylin.storage.ICachableStorageQuery;
import org.apache.kylin.storage.IStorageQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 */
public abstract class AbstractCacheFledgedStorageEngine implements IStorageQuery, TeeTupleItrListener {
    private static final Logger logger = LoggerFactory.getLogger(AbstractCacheFledgedStorageEngine.class);
    private static final String storageCacheTemplate = "StorageCache";

    protected static CacheManager CACHE_MANAGER;

    protected boolean queryCacheExists;
    protected ICachableStorageQuery underlyingStorage;
    protected StreamSQLDigest streamSQLDigest;

    public AbstractCacheFledgedStorageEngine(ICachableStorageQuery underlyingStorage) {
        this.underlyingStorage = underlyingStorage;
        this.makeCacheIfNecessary(underlyingStorage.getStorageUUID());
    }

    public static void setCacheManager(CacheManager cacheManager) {
        CACHE_MANAGER = cacheManager;
    }

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
                maxBytesLocalHeap(10, MemoryUnit.MEGABYTES).//
                persistence(new PersistenceConfiguration().strategy(PersistenceConfiguration.Strategy.NONE)));

        CACHE_MANAGER.addCache(storageCache);
    }

    private void makeCacheIfNecessary(String storageUUID) {
        if (CACHE_MANAGER == null) {
            logger.warn("CACHE_MANAGER is not provided");
            initCacheManger();
        }

        if (CACHE_MANAGER.getCache(storageUUID) == null) {
            logger.info("Cache for {} initting...", storageUUID);

            //Create a Cache specifying its configuration.
            CacheConfiguration templateConf = CACHE_MANAGER.getCache(storageCacheTemplate).getCacheConfiguration();
            PersistenceConfiguration pconf = templateConf.getPersistenceConfiguration();
            if (pconf != null) {
                logger.info("PersistenceConfiguration strategy: " + pconf.getStrategy());
            } else {
                logger.warn("PersistenceConfiguration is null");
            }

            Cache storageCache = new Cache(new CacheConfiguration(storageUUID, (int) templateConf.getMaxEntriesLocalHeap()).//
                    memoryStoreEvictionPolicy(templateConf.getMemoryStoreEvictionPolicy()).//
                    eternal(templateConf.isEternal()).//
                    timeToIdleSeconds(templateConf.getTimeToIdleSeconds()).//
                    maxBytesLocalHeap(templateConf.getMaxBytesLocalHeap(), MemoryUnit.BYTES).persistence(pconf));
            //TODO: deal with failed queries, and only cache too long query

            CACHE_MANAGER.addCache(storageCache);
        }
    }
}
