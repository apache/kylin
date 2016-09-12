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

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.metadata.realization.IRealization;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Range;

/**
 * @author xjiang
 */
public class StorageContext {
    private static final Logger logger = LoggerFactory.getLogger(StorageContext.class);

    public static final int DEFAULT_THRESHOLD = 1000000;

    private String connUrl;
    private int threshold;
    private int limit;
    private int offset;
    private int finalPushDownLimit;
    private boolean hasSort;
    private boolean acceptPartialResult;

    private boolean exactAggregation;
    private boolean needStorageAggregation;
    private boolean enableLimit;
    private boolean enableCoprocessor;

    private AtomicLong totalScanCount;
    private Cuboid cuboid;
    private boolean partialResultReturned;

    private Range<Long> reusedPeriod;

    public StorageContext() {
        this.threshold = DEFAULT_THRESHOLD;
        this.limit = DEFAULT_THRESHOLD;
        this.totalScanCount = new AtomicLong();
        this.cuboid = null;
        this.hasSort = false;

        this.exactAggregation = false;
        this.enableLimit = false;
        this.enableCoprocessor = false;

        this.acceptPartialResult = false;
        this.partialResultReturned = false;
        this.finalPushDownLimit = Integer.MAX_VALUE;
    }

    public String getConnUrl() {
        return connUrl;
    }

    public void setConnUrl(String connUrl) {
        this.connUrl = connUrl;
    }

    public int getThreshold() {
        return threshold;
    }

    public void setThreshold(int t) {
        threshold = t;
    }

    public int getLimit() {
        return limit;
    }

    public void setLimit(int l) {
        this.limit = l;
    }

    public int getOffset() {
        return offset;
    }

    public void setOffset(int offset) {
        this.offset = offset;
    }

    public void enableLimit() {
        this.enableLimit = true;
    }

    public boolean isLimitEnabled() {
        return this.enableLimit;
    }

    private int getStoragePushDownLimit() {
        return this.isLimitEnabled() ? this.getOffset() + this.getLimit() : Integer.MAX_VALUE;
    }

    public int getFinalPushDownLimit() {
        return finalPushDownLimit;
    }

    public void setFinalPushDownLimit(IRealization realization) {

        //decide the final limit push down
        int tempPushDownLimit = this.getStoragePushDownLimit();
        if (tempPushDownLimit == Integer.MAX_VALUE) {
            return;
        }
        
        int pushDownLimitMax = KylinConfig.getInstanceFromEnv().getStoragePushDownLimitMax();
        if (!realization.supportsLimitPushDown()) {
            logger.info("Not enabling limit push down because cube storage type not supported");
        } else if (tempPushDownLimit > pushDownLimitMax) {
            logger.info("Not enabling limit push down because the limit(including offset) {} is larger than kylin.query.pushdown.limit.max {}", //
                    tempPushDownLimit, pushDownLimitMax);
        } else {
            this.finalPushDownLimit = tempPushDownLimit;
        }
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

    public long getTotalScanCount() {
        return totalScanCount.get();
    }

    public long increaseTotalScanCount(long count) {
        return this.totalScanCount.addAndGet(count);
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
}
