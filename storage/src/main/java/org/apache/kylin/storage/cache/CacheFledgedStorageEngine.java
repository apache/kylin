package org.apache.kylin.storage.cache;

import java.util.List;

import javax.annotation.Nullable;

import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Element;
import net.sf.ehcache.config.CacheConfiguration;
import net.sf.ehcache.config.MemoryUnit;
import net.sf.ehcache.config.PersistenceConfiguration;
import net.sf.ehcache.store.MemoryStoreEvictionPolicy;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.RangeUtil;
import org.apache.kylin.metadata.MetadataManager;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.PartitionDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.realization.IRealization;
import org.apache.kylin.metadata.realization.RealizationType;
import org.apache.kylin.metadata.realization.SQLDigest;
import org.apache.kylin.metadata.realization.SQLDigestUtil;
import org.apache.kylin.metadata.realization.StreamSQLDigest;
import org.apache.kylin.metadata.tuple.CompoundTupleIterator;
import org.apache.kylin.metadata.tuple.ITuple;
import org.apache.kylin.metadata.tuple.ITupleIterator;
import org.apache.kylin.metadata.tuple.SimpleTupleIterator;
import org.apache.kylin.metadata.tuple.TeeTupleIterator;
import org.apache.kylin.storage.IStorageEngine;
import org.apache.kylin.storage.StorageContext;
import org.apache.kylin.storage.StorageEngineFactory;
import org.apache.kylin.storage.hbase.coprocessor.endpoint.TsConditionExtractor;
import org.apache.kylin.storage.tuple.TupleInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;

/**
 */
public class CacheFledgedStorageEngine implements IStorageEngine {

    private static final Logger logger = LoggerFactory.getLogger(CacheFledgedStorageEngine.class);

    public static final String STORAGE_LAYER_TUPLE_CACHE = "STORAGE_LAYER_TUPLE_CACHE";
    //TODO: deal with failed queries

    static CacheManager cacheManager;

    static {
        // TODO: L4J [2015-04-20 10:44:03,817][WARN][net.sf.ehcache.pool.sizeof.ObjectGraphWalker] - The configured limit of 1,000 object references was reached while attempting to calculate the size of the object graph. Severe performance degradation could occur if the sizing operation continues. This can be avoided by setting the CacheManger or Cache <sizeOfPolicy> elements maxDepthExceededBehavior to "abort" or adding stop points with @IgnoreSizeOf annotations. If performance degradation is NOT an issue at the configured limit, raise the limit value using the CacheManager or Cache <sizeOfPolicy
        cacheManager = CacheManager.create();

        //Create a Cache specifying its configuration.
        Cache successCache = new Cache(new CacheConfiguration(STORAGE_LAYER_TUPLE_CACHE, 0).//
                memoryStoreEvictionPolicy(MemoryStoreEvictionPolicy.LRU).//
                eternal(false).//
                timeToIdleSeconds(86400).//
                diskExpiryThreadIntervalSeconds(0).//
                maxBytesLocalHeap(1, MemoryUnit.GIGABYTES).//
                persistence(new PersistenceConfiguration().strategy(PersistenceConfiguration.Strategy.NONE)));

        cacheManager.addCache(successCache);
    }

    private TblColRef partitionColRef;
    private IRealization realization;

    public CacheFledgedStorageEngine(IRealization realization) {
        this.realization = realization;

        Preconditions.checkArgument(realization.getType() != RealizationType.CUBE, "Cube realization does not need dynamic cache!");
        String modelName = realization.getModelName();
        DataModelDesc dataModelDesc = MetadataManager.getInstance(KylinConfig.getInstanceFromEnv()).getDataModelDesc(modelName);
        PartitionDesc partitionDesc = dataModelDesc.getPartitionDesc();
        Preconditions.checkArgument(partitionDesc != null, "PartitionDesc for " + realization + " is null!");
        assert partitionDesc != null;
        partitionColRef = partitionDesc.getPartitionDateColumnRef();
        Preconditions.checkArgument(partitionColRef != null, "getPartitionDateColumnRef for " + realization + " is null");
    }

    @Override
    public ITupleIterator search(final StorageContext context, final SQLDigest sqlDigest, final TupleInfo returnTupleInfo) {

        //enable storage layer cache iff ts column is contained in filter
        boolean needUpdateCache = sqlDigest.groupbyColumns.contains(partitionColRef);

        final StreamSQLDigest streamSQLDigest = new StreamSQLDigest(sqlDigest, partitionColRef);
        StreamSQLResult cachedResult = null;
        Cache cache = cacheManager.getCache(STORAGE_LAYER_TUPLE_CACHE);
        Element element = cache.get(streamSQLDigest);
        if (element != null) {
            cachedResult = (StreamSQLResult) element.getObjectValue();
        }

        Range<Long> ts = TsConditionExtractor.extractTsCondition(partitionColRef, sqlDigest.filter);
        if (ts == null || ts.isEmpty()) {
            logger.info("ts range in the query conflicts,return empty directly");
            return ITupleIterator.EMPTY_TUPLE_ITERATOR;
        }

        ITupleIterator ret = null;
        if (cachedResult != null) {
            logger.debug("existing cache    : " + cachedResult);
            Range<Long> reusePeriod = cachedResult.getReusableResults(ts);

            logger.info("ts Range in query: " + RangeUtil.formatTsRange(ts));
            logger.info("potential reusable range   : " + RangeUtil.formatTsRange(reusePeriod));

            if (reusePeriod != null) {

                List<Range<Long>> remainings = RangeUtil.remove(ts, reusePeriod);
                if (remainings.size() == 1) {

                    SimpleTupleIterator reusedTuples = new SimpleTupleIterator(cachedResult.reuse(reusePeriod));
                    Range<Long> remaining = remainings.get(0);
                    logger.info("Appending ts " + RangeUtil.formatTsRange(remaining) + " as additional filter");
                    ITupleIterator freshTuples = SQLDigestUtil.appendTsFilterToExecute(sqlDigest, partitionColRef, remaining, new Function<Void, ITupleIterator>() {
                        @Override
                        public ITupleIterator apply(Void input) {
                            return StorageEngineFactory.getStorageEngine(realization, false).search(context, sqlDigest, returnTupleInfo);
                        }
                    });

                    ret = new CompoundTupleIterator(Lists.newArrayList(reusedTuples, freshTuples));
                } else if (remainings.size() == 0) {
                    needUpdateCache = false;
                    ret = new SimpleTupleIterator(cachedResult.reuse(reusePeriod));
                }
                //if remaining size > 1, we skip using cache , i.e, ret will == null
            }
        } else {
            logger.info("no cache entry for this query");
        }

        if (ret == null) {
            logger.info("decision: not using cache");
            //cache cannot reuse case:
            ret = StorageEngineFactory.getStorageEngine(realization, false).search(context, sqlDigest, returnTupleInfo);
        } else {
            logger.info("decision: use cache");
        }

        if (needUpdateCache) {
            //the tsRange in cache should reflect data aliveness
            final Range<Long> finalTs = ts;

            //use another nested ITupleIterator to deal with cache
            final TeeTupleIterator tee = new TeeTupleIterator(ret);
            tee.setActionOnSeeingWholeData(new Function<List<ITuple>, Void>() {
                @Nullable
                @Override
                public Void apply(List<ITuple> input) {
                    Range<Long> tsRange = finalTs;
                    Range<Long> cacheExclude = tee.getCacheExcludedPeriod();
                    if (cacheExclude != null) {
                        List<Range<Long>> cachablePeriods = RangeUtil.remove(tsRange, cacheExclude);
                        if (cachablePeriods.size() == 1) {
                            if (!tsRange.equals(cachablePeriods.get(0))) {
                                logger.info("With respect to each shard's build status, the cacheable tsRange shrinks from " + RangeUtil.formatTsRange(tsRange) + " to " + RangeUtil.formatTsRange(cachablePeriods.get(0)));
                            }
                            tsRange = cachablePeriods.get(0);
                        } else {
                            //give up updating the cache, in avoid to make cache complicated
                            return null;
                        }
                    }

                    StreamSQLResult newCacheEntry = new StreamSQLResult(input, tsRange, partitionColRef);
                    cacheManager.getCache(STORAGE_LAYER_TUPLE_CACHE).put(new Element(streamSQLDigest, newCacheEntry));
                    logger.debug("cache after the query: " + newCacheEntry);
                    return null;
                }
            });

            return tee;
        } else {
            return ret;
        }
    }
}
