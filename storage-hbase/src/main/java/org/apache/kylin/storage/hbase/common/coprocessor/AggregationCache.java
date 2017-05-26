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

package org.apache.kylin.storage.hbase.common.coprocessor;

import java.util.Map;

import org.apache.kylin.common.util.MemoryBudgetController;
import org.apache.kylin.measure.MeasureAggregator;

import com.google.common.collect.Maps;

/**
 */
@SuppressWarnings("rawtypes")
public abstract class AggregationCache {
    static final long MEMORY_USAGE_CAP = 500 * 1024 * 1024L; // 500 MB
    static final long MEMOERY_MAX_BYTES = Runtime.getRuntime().maxMemory();
    protected final Map<AggrKey, MeasureAggregator[]> aggBufMap;
    transient int rowMemBytes;
    private AggrKey firstKey = null;

    public AggregationCache() {
        this.aggBufMap = Maps.newHashMap();
    }

    public abstract MeasureAggregator[] createBuffer();

    public MeasureAggregator[] getBuffer(AggrKey aggkey) {
        MeasureAggregator[] aggBuf = aggBufMap.get(aggkey);
        if (aggBuf == null) {
            aggBuf = createBuffer();
            AggrKey key = aggkey.copy();
            aggBufMap.put(key, aggBuf);

            if (firstKey == null) {
                firstKey = key;
            }
        }
        return aggBuf;
    }

    public long getSize() {
        return aggBufMap.size();
    }

    public void checkMemoryUsage() {
        if (firstKey == null)
            return;

        // about memory calculation,
        // http://seniorjava.wordpress.com/2013/09/01/java-objects-memory-size-reference/
        if (rowMemBytes <= 0) {
            if (aggBufMap.size() > 0) {
                rowMemBytes = 0;
                MeasureAggregator[] measureAggregators = aggBufMap.get(firstKey);
                for (MeasureAggregator agg : measureAggregators) {
                    rowMemBytes += agg.getMemBytesEstimate();
                }
            }
        }
        int size = aggBufMap.size();
        long memUsage = (40L + rowMemBytes) * size;
        if (memUsage > MEMORY_USAGE_CAP) {
            throw new RuntimeException("Kylin coprocessor memory usage goes beyond cap, (40 + " + rowMemBytes + ") * " + size + " > " + MEMORY_USAGE_CAP + ". Abort coprocessor.");
        }

        //If less than 5% of max memory
        long avail = MemoryBudgetController.getSystemAvailBytes();
        if (avail < (MEMOERY_MAX_BYTES / 20)) {
            throw new RuntimeException("Running Kylin coprocessor when too little memory is left. Abort coprocessor. Current available memory is " + avail + ". Max memory is " + MEMOERY_MAX_BYTES);
        }
    }
}
