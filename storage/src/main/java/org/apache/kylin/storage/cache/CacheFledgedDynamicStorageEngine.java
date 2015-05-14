package org.apache.kylin.storage.cache;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import net.sf.ehcache.Cache;
import net.sf.ehcache.Element;
import org.apache.kylin.common.util.RangeUtil;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.realization.SQLDigest;
import org.apache.kylin.metadata.realization.SQLDigestUtil;
import org.apache.kylin.metadata.realization.StreamSQLDigest;
import org.apache.kylin.metadata.tuple.*;
import org.apache.kylin.storage.ICachableStorageEngine;
import org.apache.kylin.storage.StorageContext;
import org.apache.kylin.storage.hbase.coprocessor.endpoint.TsConditionExtractor;
import org.apache.kylin.storage.tuple.TupleInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Created by Hongbin Ma(Binmahone) on 5/11/15.
 */
public class CacheFledgedDynamicStorageEngine extends AbstractCacheFledgedStorageEngine {
    private static final Logger logger = LoggerFactory.getLogger(CacheFledgedDynamicStorageEngine.class);

    private final TblColRef partitionColRef;

    private Range<Long> ts;

    public CacheFledgedDynamicStorageEngine(ICachableStorageEngine underlyingStorage, TblColRef partitionColRef) {
        super(underlyingStorage);
        this.partitionColRef = partitionColRef;

        Preconditions.checkArgument(this.partitionColRef != null, "For dynamic columns like " + //
                this.underlyingStorage.getStorageUUID()+ ", partition column must be provided");
    }

    @Override
    public ITupleIterator search(final StorageContext context, final SQLDigest sqlDigest, final TupleInfo returnTupleInfo) {
        //enable dynamic cache iff group by columns contains partition col
        //because cache extraction requires partition col value as selection key
        boolean needUpdateCache = sqlDigest.groupbyColumns.contains(partitionColRef);

        streamSQLDigest = new StreamSQLDigest(sqlDigest, partitionColRef);
        StreamSQLResult cachedResult = null;
        Cache cache = cacheManager.getCache(this.underlyingStorage.getStorageUUID());
        Element element = cache.get(streamSQLDigest);
        if (element != null) {
            this.queryCacheExists = true;
            cachedResult = (StreamSQLResult) element.getObjectValue();
        }

        ts = TsConditionExtractor.extractTsCondition(partitionColRef, sqlDigest.filter);
        if (ts == null || ts.isEmpty()) {
            logger.info("ts range in the query conflicts,return empty directly");
            return ITupleIterator.EMPTY_TUPLE_ITERATOR;
        }

        ITupleIterator ret = null;
        if (cachedResult != null) {
            Range<Long> reusePeriod = cachedResult.getReusableResults(ts);

            logger.info("existing cache    : " + cachedResult);
            logger.info("ts Range in query: " + RangeUtil.formatTsRange(ts));
            logger.info("potential reusable range   : " + RangeUtil.formatTsRange(reusePeriod));

            if (reusePeriod != null) {
                List<Range<Long>> remainings = RangeUtil.remove(ts, reusePeriod);
                if (remainings.size() == 1) {//if using cache causes two underlyingStorage searches, we'd rather not use the cache

                    SimpleTupleIterator reusedTuples = new SimpleTupleIterator(cachedResult.reuse(reusePeriod));
                    List<ITupleIterator> iTupleIteratorList = Lists.newArrayList();
                    iTupleIteratorList.add(reusedTuples);

                    for (Range<Long> remaining : remainings) {
                        logger.info("Appending ts " + RangeUtil.formatTsRange(remaining) + " as additional filter");

                        ITupleIterator freshTuples = SQLDigestUtil.appendTsFilterToExecute(sqlDigest, partitionColRef, remaining, new Function<Void, ITupleIterator>() {
                            @Override
                            public ITupleIterator apply(Void input) {
                                return underlyingStorage.search(context, sqlDigest, returnTupleInfo);
                            }
                        });
                        iTupleIteratorList.add(freshTuples);
                    }

                    ret = new CompoundTupleIterator(iTupleIteratorList);
                } else if (remainings.size() == 0) {
                    needUpdateCache = false;
                    ret = new SimpleTupleIterator(cachedResult.reuse(reusePeriod));
                } else {
                    //if using cache causes two underlyingStorage searches, we'd rather not use the cache
                }
            }
        } else {
            logger.info("no cache entry for this query");
        }

        if (ret == null) {
            logger.info("decision: not using cache");
            ret = underlyingStorage.search(context, sqlDigest, returnTupleInfo);
        } else {
            logger.info("decision: use cache");
        }

        if (needUpdateCache) {
            //use another nested ITupleIterator to deal with cache
            final TeeTupleIterator tee = new TeeTupleIterator(ret);
            tee.addCloseListener(this);
            return tee;
        } else {
            return ret;
        }
    }

   @Override
    public void notify(List<ITuple> duplicated) {
        Range<Long> cacheExclude = this.underlyingStorage.getVolatilePeriod();
        if (cacheExclude != null) {
            List<Range<Long>> cachablePeriods = RangeUtil.remove(ts, cacheExclude);
            if (cachablePeriods.size() == 1) {
                if (!ts.equals(cachablePeriods.get(0))) {
                    logger.info("With respect to each shard's build status, the cacheable tsRange shrinks from " + RangeUtil.formatTsRange(ts) + " to " + RangeUtil.formatTsRange(cachablePeriods.get(0)));
                }
                ts = cachablePeriods.get(0);
            } else {
                //give up updating the cache, in avoid to make cache complicated
            }
        }

        StreamSQLResult newCacheEntry = new StreamSQLResult(duplicated, ts, partitionColRef);
        cacheManager.getCache(this.underlyingStorage.getStorageUUID()).put(new Element(streamSQLDigest, newCacheEntry));
        logger.info("cache after the query: " + newCacheEntry);
    }
}
