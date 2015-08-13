package org.apache.kylin.storage.cache;

import java.util.List;

import net.sf.ehcache.Cache;
import net.sf.ehcache.Element;

import org.apache.kylin.metadata.realization.SQLDigest;
import org.apache.kylin.metadata.realization.StreamSQLDigest;
import org.apache.kylin.metadata.tuple.ITuple;
import org.apache.kylin.metadata.tuple.ITupleIterator;
import org.apache.kylin.metadata.tuple.SimpleTupleIterator;
import org.apache.kylin.metadata.tuple.TeeTupleIterator;
import org.apache.kylin.storage.ICachableStorageQuery;
import org.apache.kylin.storage.StorageContext;
import org.apache.kylin.storage.tuple.TupleInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Ranges;

public class CacheFledgedStaticStorageEngine extends AbstractCacheFledgedStorageEngine {
    private static final Logger logger = LoggerFactory.getLogger(CacheFledgedStaticStorageEngine.class);

    public CacheFledgedStaticStorageEngine(ICachableStorageQuery underlyingStorage) {
        super(underlyingStorage);
    }

    @Override
    public ITupleIterator search(final StorageContext context, final SQLDigest sqlDigest, final TupleInfo returnTupleInfo) {

        streamSQLDigest = new StreamSQLDigest(sqlDigest, null);
        StreamSQLResult cachedResult = null;
        Cache cache = CACHE_MANAGER.getCache(this.underlyingStorage.getStorageUUID());
        Element element = cache.get(streamSQLDigest.hashCode());
        if (element != null) {
            this.queryCacheExists = true;
            cachedResult = (StreamSQLResult) element.getObjectValue();
        }

        ITupleIterator ret = null;
        if (cachedResult != null) {
            ret = new SimpleTupleIterator(cachedResult.reuse(Ranges.<Long> all()));
        } else {
            logger.info("no cache entry for this query");
        }

        if (ret == null) {
            logger.info("decision: not using cache");
            ret = underlyingStorage.search(context, sqlDigest, returnTupleInfo);
        } else {
            logger.info("decision: use cache");
        }

        if (!queryCacheExists) {
            //use another nested ITupleIterator to deal with cache
            final TeeTupleIterator tee = new TeeTupleIterator(ret);
            tee.addCloseListener(this);
            return tee;
        } else {
            return ret;
        }
    }

    @Override
    public void notify(List<ITuple> duplicated, long createTime) {
        boolean cacheIt = true;
        //        long storageQueryTime = System.currentTimeMillis() - createTime;
        //        long durationThreshold = KylinConfig.getInstanceFromEnv().getQueryDurationCacheThreshold();
        //        long scancountThreshold = KylinConfig.getInstanceFromEnv().getQueryScanCountCacheThreshold();
        //
        //        if (storageQueryTime < durationThreshold) {
        //            logger.info("Skip storage caching for storage cache because storage query time {} less than {}", storageQueryTime, durationThreshold);
        //            cacheIt = false;
        //        }
        //
        //        if (duplicated.size() < scancountThreshold) {
        //            logger.info("Skip storage caching for storage cache because scan count {} less than {}", duplicated.size(), scancountThreshold);
        //            cacheIt = false;
        //        }

        if (cacheIt) {
            StreamSQLResult newCacheEntry = new StreamSQLResult(duplicated, Ranges.<Long> all(), null);
            CACHE_MANAGER.getCache(this.underlyingStorage.getStorageUUID()).put(new Element(streamSQLDigest.hashCode(), newCacheEntry));
            logger.info("cache after the query: " + newCacheEntry);
        }
    }
}
