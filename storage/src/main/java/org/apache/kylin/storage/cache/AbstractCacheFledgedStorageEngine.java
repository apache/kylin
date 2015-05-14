package org.apache.kylin.storage.cache;

import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.config.CacheConfiguration;
import net.sf.ehcache.config.Configuration;
import net.sf.ehcache.config.PersistenceConfiguration;
import net.sf.ehcache.store.MemoryStoreEvictionPolicy;
import org.apache.kylin.metadata.realization.StreamSQLDigest;
import org.apache.kylin.metadata.tuple.TeeTupleItrListener;
import org.apache.kylin.storage.ICachableStorageEngine;
import org.apache.kylin.storage.IStorageEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Hongbin Ma(Binmahone) on 5/13/15.
 */
public abstract class AbstractCacheFledgedStorageEngine implements IStorageEngine, TeeTupleItrListener {
    private static final Logger logger = LoggerFactory.getLogger(AbstractCacheFledgedStorageEngine.class);
    protected static CacheManager cacheManager;

    static {
        Configuration conf = new Configuration();
        conf.setMaxBytesLocalHeap("1024M");
        cacheManager = CacheManager.create(conf);
    }

    protected final ICachableStorageEngine underlyingStorage;
    protected StreamSQLDigest streamSQLDigest;
    protected boolean queryCacheExists;

    public AbstractCacheFledgedStorageEngine(ICachableStorageEngine underlyingStorage) {
        this.underlyingStorage = underlyingStorage;
        this.queryCacheExists = false;
        this.makeCacheIfNecessary(underlyingStorage.getStorageUUID());
    }

    private void makeCacheIfNecessary(String realizationUUID) {
        if (cacheManager.getCache(realizationUUID) == null) {
            logger.info("Cache for {} initting...", realizationUUID);
            // TODO: L4J [2015-04-20 10:44:03,817][WARN][net.sf.ehcache.pool.sizeof.ObjectGraphWalker] - The configured limit of 1,000 object references was reached while attempting to calculate the size of the object graph. Severe performance degradation could occur if the sizing operation continues. This can be avoided by setting the CacheManger or Cache <sizeOfPolicy> elements maxDepthExceededBehavior to "abort" or adding stop points with @IgnoreSizeOf annotations. If performance degradation is NOT an issue at the configured limit, raise the limit value using the CacheManager or Cache <sizeOfPolicy
            //Create a Cache specifying its configuration.
            Cache storageCache = new Cache(new CacheConfiguration(realizationUUID, 0).//
                    memoryStoreEvictionPolicy(MemoryStoreEvictionPolicy.LRU).//
                    eternal(false).//
                    timeToIdleSeconds(86400).//
                    diskExpiryThreadIntervalSeconds(0).//
                    //maxBytesLocalHeap(256, MemoryUnit.MEGABYTES).//already defined at manager scope
                    persistence(new PersistenceConfiguration().strategy(PersistenceConfiguration.Strategy.NONE)));
            //TODO: deal with failed queries, and only cache too long query

            cacheManager.addCache(storageCache);
        }
    }
}
