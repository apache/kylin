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

package org.apache.kylin.storage.hbase.coprocessor;

import com.google.common.collect.Maps;
import org.apache.kylin.metadata.measure.MeasureAggregator;

import java.util.SortedMap;

/**
 * Created by Hongbin Ma(Binmahone) on 11/27/14.
 */
@SuppressWarnings("rawtypes")
public abstract class AggregationCache {
    transient int rowMemBytes;
    static final int MEMORY_USAGE_CAP = 500 * 1024 * 1024; // 500 MB
    protected final SortedMap<CoprocessorProjector.AggrKey, MeasureAggregator[]> aggBufMap;

    public AggregationCache() {
        this.aggBufMap = Maps.newTreeMap();
    }

    public abstract MeasureAggregator[] createBuffer();

    public MeasureAggregator[] getBuffer(CoprocessorProjector.AggrKey aggkey) {
        MeasureAggregator[] aggBuf = aggBufMap.get(aggkey);
        if (aggBuf == null) {
            aggBuf = createBuffer();
            aggBufMap.put(aggkey.copy(), aggBuf);
        }
        return aggBuf;
    }

    public long getSize() {
        return aggBufMap.size();
    }

    public void checkMemoryUsage() {
        // about memory calculation,
        // http://seniorjava.wordpress.com/2013/09/01/java-objects-memory-size-reference/
        if (rowMemBytes <= 0) {
            if (aggBufMap.size() > 0) {
                rowMemBytes = 0;
                MeasureAggregator[] measureAggregators = aggBufMap.get(aggBufMap.firstKey());
                for (MeasureAggregator agg : measureAggregators) {
                    rowMemBytes += agg.getMemBytes();
                }
            }
        }
        int size = aggBufMap.size();
        int memUsage = (40 + rowMemBytes) * size;
        if (memUsage > MEMORY_USAGE_CAP) {
            throw new RuntimeException("Kylin coprocess memory usage goes beyond cap, (40 + " + rowMemBytes + ") * " + size + " > " + MEMORY_USAGE_CAP + ". Abort coprocessor.");
        }
    }
}
