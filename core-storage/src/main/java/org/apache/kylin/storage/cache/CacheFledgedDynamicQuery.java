/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kylin.storage.cache;

import java.util.List;

import net.sf.ehcache.Element;

import org.apache.kylin.common.util.RangeUtil;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.realization.SQLDigest;
import org.apache.kylin.metadata.realization.SQLDigestUtil;
import org.apache.kylin.metadata.realization.StreamSQLDigest;
import org.apache.kylin.metadata.tuple.CompoundTupleIterator;
import org.apache.kylin.metadata.tuple.ITuple;
import org.apache.kylin.metadata.tuple.ITupleIterator;
import org.apache.kylin.metadata.tuple.SimpleTupleIterator;
import org.apache.kylin.metadata.tuple.TeeTupleIterator;
import org.apache.kylin.storage.ICachableStorageQuery;
import org.apache.kylin.storage.StorageContext;
import org.apache.kylin.storage.tuple.TupleInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;

/**
 */
public class CacheFledgedDynamicQuery extends AbstractCacheFledgedQuery {
    private static final Logger logger = LoggerFactory.getLogger(CacheFledgedDynamicQuery.class);

    private final TblColRef partitionColRef;

    private boolean noCacheUsed = true;

    private Range<Long> ts;

    public CacheFledgedDynamicQuery(ICachableStorageQuery underlyingStorage, TblColRef partitionColRef) {
        super(underlyingStorage);
        this.partitionColRef = partitionColRef;

        Preconditions.checkArgument(this.partitionColRef != null, "For dynamic columns like " + //
                this.underlyingStorage.getStorageUUID() + ", partition column must be provided");
    }

    @Override
    public ITupleIterator search(final StorageContext context, final SQLDigest sqlDigest, final TupleInfo returnTupleInfo) {
        //check if ts condition in sqlDigest valid
        ts = TsConditionExtractor.extractTsCondition(partitionColRef, sqlDigest.filter);
        if (ts == null || ts.isEmpty()) {
            logger.info("ts range in the query conflicts,return empty directly");
            return ITupleIterator.EMPTY_TUPLE_ITERATOR;
        }

        ITupleIterator ret = null;

        //enable dynamic cache iff group by columns contains partition col
        //because cache extraction requires partition col value as selection key
        boolean enableDynamicCache = sqlDigest.groupbyColumns.contains(partitionColRef);

        if (enableDynamicCache) {
            streamSQLDigest = new StreamSQLDigest(sqlDigest, partitionColRef);
            StreamSQLResult cachedResult = getStreamSQLResult(streamSQLDigest);
            if (cachedResult != null) {
                ret = tryReuseCache(context, sqlDigest, returnTupleInfo, cachedResult);
            } else {
                logger.info("no cache entry for this query");
            }
        }

        if (ret == null) {
            ret = underlyingStorage.search(context, sqlDigest, returnTupleInfo);
            logger.info("No Cache being used");
        } else {
            logger.info("Cache being used");
        }

        if (enableDynamicCache) {
            //use another nested ITupleIterator to deal with cache
            final TeeTupleIterator tee = new TeeTupleIterator(ret);
            tee.addCloseListener(this);
            return tee;
        } else {
            return ret;
        }
    }

    /**
     * if cache is not enough it will try to combine existing cache as well as fresh records
     */
    private ITupleIterator tryReuseCache(final StorageContext context, final SQLDigest sqlDigest, final TupleInfo returnTupleInfo, StreamSQLResult cachedResult) {
        Range<Long> reusePeriod = cachedResult.getReusableResults(ts);

        logger.info("existing cache: " + cachedResult);
        logger.info("ts Range in query: " + RangeUtil.formatTsRange(ts));
        logger.info("potential reusable range: " + RangeUtil.formatTsRange(reusePeriod));

        if (reusePeriod != null) {
            List<Range<Long>> remainings = RangeUtil.remove(ts, reusePeriod);
            if (remainings.size() == 1) {//if using cache causes two underlyingStorage searches, we'd rather not use the cache

                SimpleTupleIterator reusedTuples = new SimpleTupleIterator(cachedResult.reuse(reusePeriod));
                List<ITupleIterator> iTupleIteratorList = Lists.newArrayList();
                iTupleIteratorList.add(reusedTuples);

                Range<Long> remaining = remainings.get(0);
                logger.info("Appending ts " + RangeUtil.formatTsRange(remaining) + " as additional filter");

                ITupleIterator freshTuples = SQLDigestUtil.appendTsFilterToExecute(sqlDigest, partitionColRef, remaining, new Function<Void, ITupleIterator>() {
                    @Override
                    public ITupleIterator apply(Void input) {
                        return underlyingStorage.search(context, sqlDigest, returnTupleInfo);
                    }
                });
                iTupleIteratorList.add(freshTuples);

                context.setReusedPeriod(reusePeriod);
                return new CompoundTupleIterator(iTupleIteratorList);
            } else if (remainings.size() == 0) {
                logger.info("The ts range in new query was fully cached");
                context.setReusedPeriod(reusePeriod);
                return new SimpleTupleIterator(cachedResult.reuse(reusePeriod));
            } else {
                //if using cache causes more than one underlyingStorage searches
                //the incurred overhead might be more expensive than the cache benefit
                logger.info("Give up using cache to avoid complexity");
                return null;
            }
        } else {
            logger.info("cached results not reusable by current query");
            return null;
        }
    }

    @Override
    public void notify(List<ITuple> duplicated, long createTime) {

        //for streaming sql only check if needSaveCache at first entry of cache
        if (noCacheUsed && !needSaveCache(createTime)) {
            return;
        }

        Range<Long> cacheExclude = this.underlyingStorage.getVolatilePeriod();
        if (cacheExclude != null) {
            List<Range<Long>> cachablePeriods = RangeUtil.remove(ts, cacheExclude);
            if (cachablePeriods.size() == 1) {
                if (!ts.equals(cachablePeriods.get(0))) {
                    logger.info("tsRange shrinks from " + RangeUtil.formatTsRange(ts) + " to " + RangeUtil.formatTsRange(cachablePeriods.get(0)));
                }
                ts = cachablePeriods.get(0);
            } else {
                //give up updating the cache, in avoid to make cache complicated
                logger.info("Skip updating cache to avoid complexity");
            }
        }

        StreamSQLResult newCacheEntry = new StreamSQLResult(duplicated, ts, partitionColRef);
        CACHE_MANAGER.getCache(this.underlyingStorage.getStorageUUID()).put(new Element(streamSQLDigest.hashCode(), newCacheEntry));
        logger.info("cache after the query: " + newCacheEntry);
    }
}
