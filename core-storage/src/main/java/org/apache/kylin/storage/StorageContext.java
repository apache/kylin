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

package org.apache.kylin.storage;

import java.util.concurrent.atomic.AtomicLong;

import org.apache.kylin.common.StorageURL;
import org.apache.kylin.common.debug.BackdoorToggles;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.cube.gridtable.CuboidToGridTableMapping;
import org.apache.kylin.gridtable.StorageLimitLevel;
import org.apache.kylin.metadata.realization.IRealization;
import org.apache.kylin.storage.gtrecord.GTCubeStorageQueryBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kylin.shaded.com.google.common.collect.Range;

/**
 * @author xjiang
 */
public class StorageContext {
    private static final Logger logger = LoggerFactory.getLogger(StorageContext.class);

    public final int ctxId;

    private StorageURL connUrl;
    private int limit = Integer.MAX_VALUE;
    private boolean overlookOuterLimit = false;
    private int offset = 0;
    private int finalPushDownLimit = Integer.MAX_VALUE;
    private StorageLimitLevel storageLimitLevel = StorageLimitLevel.NO_LIMIT;
    private boolean hasSort = false;
    private boolean acceptPartialResult = false;

    private boolean exactAggregation = false;
    private boolean needStorageAggregation = false;
    private boolean enableCoprocessor = false;
    private boolean enableStreamAggregate = false;

    private IStorageQuery storageQuery;
    private AtomicLong processedRowCount = new AtomicLong();
    private Cuboid cuboid;
    private CuboidToGridTableMapping mapping;
    private boolean partialResultReturned = false;

    private Range<Long> reusedPeriod;

    private long filterMask;

    public StorageContext() {
        this(0);
    }

    public StorageContext(int ctxId) {
        this.ctxId = ctxId;
    }

    public long getFilterMask() {
        return filterMask;
    }

    public void setFilterMask(long filterMask) {
        this.filterMask = filterMask;
    }

    public StorageURL getConnUrl() {
        return connUrl;
    }

    public void setConnUrl(StorageURL connUrl) {
        this.connUrl = connUrl;
    }

    //the limit here correspond to the limit concept in SQL
    //also take into consideration Statement.setMaxRows in JDBC
    private int getLimit() {
        if (overlookOuterLimit || BackdoorToggles.getStatementMaxRows() == null
                || BackdoorToggles.getStatementMaxRows() == 0) {
            return limit;
        } else {
            return Math.min(limit, BackdoorToggles.getStatementMaxRows());
        }
    }

    public void setLimit(int l) {
        if (limit != Integer.MAX_VALUE) {
            logger.warn("Setting limit to {} but in current olap context, the limit is already {}, won't apply", l,
                    limit);
        } else {
            limit = l;
        }
    }

    //outer limit is sth like Statement.setMaxRows in JDBC
    public void setOverlookOuterLimit() {
        this.overlookOuterLimit = true;
    }

    //the offset here correspond to the offset concept in SQL
    public int getOffset() {
        return offset;
    }

    public void setOffset(int offset) {
        this.offset = offset;
    }

    /**
     * in contrast to the limit in SQL concept, "limit push down" means
     * whether the limit is effective in storage level. Some queries are not possible 
     * to leverage limit clause, checkout 
     * {@link GTCubeStorageQueryBase#enableStorageLimitIfPossible(org.apache.kylin.cube.cuboid.Cuboid, java.util.Collection, java.util.Set, java.util.Collection, org.apache.kylin.metadata.filter.TupleFilter, java.util.Set, java.util.Collection, org.apache.kylin.storage.StorageContext)}
     */
    public boolean isLimitPushDownEnabled() {
        return isValidPushDownLimit(finalPushDownLimit);
    }

    public static boolean isValidPushDownLimit(long finalPushDownLimit) {
        return finalPushDownLimit < Integer.MAX_VALUE && finalPushDownLimit > 0;
    }

    public int getFinalPushDownLimit() {
        return finalPushDownLimit;
    }

    public StorageLimitLevel getStorageLimitLevel() {
        return storageLimitLevel;
    }

    public void applyLimitPushDown(IRealization realization, StorageLimitLevel storageLimitLevel) {

        if (storageLimitLevel == StorageLimitLevel.NO_LIMIT) {
            return;
        }

        if (!realization.supportsLimitPushDown()) {
            logger.warn("Not enabling limit push down because cube storage type not supported");
            return;
        }

        long temp = this.getOffset() + (long) this.getLimit();

        if (!isValidPushDownLimit(temp)) {
            logger.warn("Not enabling limit push down because current limit is invalid: " + this.getLimit());
            return;
        }

        this.finalPushDownLimit = (int) temp;
        this.storageLimitLevel = storageLimitLevel;
        logger.info("Enabling limit push down: {} at level: {}", temp, storageLimitLevel);
    }

    public boolean mergeSortPartitionResults() {
        return mergeSortPartitionResults(finalPushDownLimit);
    }

    public static boolean mergeSortPartitionResults(int finalPushDownLimit) {
        return isValidPushDownLimit(finalPushDownLimit);
    }

    public void markSort() {
        this.hasSort = true;
    }

    public boolean hasSort() {
        return this.hasSort;
    }

    public void setCuboid(Cuboid c) {
        cuboid = c;
    }

    public Cuboid getCuboid() {
        return cuboid;
    }

    public CuboidToGridTableMapping getMapping() {
        return mapping;
    }

    public void setMapping(CuboidToGridTableMapping mapping) {
        this.mapping = mapping;
    }

    public long getProcessedRowCount() {
        return processedRowCount.get();
    }

    public long increaseProcessedRowCount(long count) {
        return processedRowCount.addAndGet(count);
    }

    public boolean isAcceptPartialResult() {
        return acceptPartialResult;
    }

    public void setAcceptPartialResult(boolean acceptPartialResult) {
        this.acceptPartialResult = acceptPartialResult;
    }

    public boolean isPartialResultReturned() {
        return partialResultReturned;
    }

    public void setPartialResultReturned(boolean partialResultReturned) {
        this.partialResultReturned = partialResultReturned;
    }

    public boolean isNeedStorageAggregation() {
        return needStorageAggregation;
    }

    public void setNeedStorageAggregation(boolean needStorageAggregation) {
        this.needStorageAggregation = needStorageAggregation;
    }

    public void setExactAggregation(boolean isExactAggregation) {
        this.exactAggregation = isExactAggregation;
    }

    public boolean isExactAggregation() {
        return this.exactAggregation;
    }

    public void enableCoprocessor() {
        this.enableCoprocessor = true;
    }

    public boolean isCoprocessorEnabled() {
        return this.enableCoprocessor;
    }

    public Range<Long> getReusedPeriod() {
        return reusedPeriod;
    }

    public void setReusedPeriod(Range<Long> reusedPeriod) {
        this.reusedPeriod = reusedPeriod;
    }

    public IStorageQuery getStorageQuery() {
        return storageQuery;
    }

    public void setStorageQuery(IStorageQuery storageQuery) {
        this.storageQuery = storageQuery;
    }

    public boolean isStreamAggregateEnabled() {
        return enableStreamAggregate;
    }

    public void enableStreamAggregate() {
        this.enableStreamAggregate = true;
    }
}
