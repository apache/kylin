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

package org.apache.kylin.rest.metrics;

import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.metrics2.lib.MutableQuantiles;
import org.apache.hadoop.metrics2.lib.MutableRate;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;

import javax.annotation.concurrent.ThreadSafe;

/**
 * properties and methods about query.
 */

@ThreadSafe
@Metrics(name = "Query", about = "Query metrics", context = "Kylin")
public class QueryMetrics {

    final MetricsRegistry registry = new MetricsRegistry("Query");

    @Metric
    MutableCounterLong querySuccessCount;
    @Metric
    MutableCounterLong queryFailCount;
    @Metric
    MutableCounterLong queryCount;
    @Metric
    MutableCounterLong cacheHitCount;
    MutableQuantiles[] cacheHitCountQuantiles;

    @Metric
    MutableRate queryLatency;
    MutableQuantiles[] queryLatencyTimeMillisQuantiles;

    @Metric
    MutableRate scanRowCount;
    MutableQuantiles[] scanRowCountQuantiles;

    @Metric
    MutableRate resultRowCount;
    MutableQuantiles[] resultRowCountQuantiles;

    public QueryMetrics(int[] intervals) {
        queryLatencyTimeMillisQuantiles = new MutableQuantiles[intervals.length];
        scanRowCountQuantiles = new MutableQuantiles[intervals.length];
        resultRowCountQuantiles = new MutableQuantiles[intervals.length];
        cacheHitCountQuantiles = new MutableQuantiles[intervals.length];

        for (int i = 0; i < intervals.length; i++) {
            int interval = intervals[i];

            queryLatencyTimeMillisQuantiles[i] = registry.newQuantiles("QueryLatency" + interval + "s", "Query queue time in milli second", "ops", "", interval);
            scanRowCountQuantiles[i] = registry.newQuantiles("ScanRowCount" + interval + "s", "Scan row count in milli second", "ops", "", interval);
            resultRowCountQuantiles[i] = registry.newQuantiles("ResultRowCount" + interval + "s", "Result row count in milli second", "ops", "", interval);
            cacheHitCountQuantiles[i] = registry.newQuantiles("CacheHitCount" + interval + "s", "Cache Hit Count in milli second", "ops", "", interval);
        }

        queryLatency = registry.newRate("QueryLatency", "", true);
        scanRowCount = registry.newRate("ScanRowCount", "", true);
        resultRowCount = registry.newRate("ResultRowCount", "", true);
    }

    public void shutdown() {
        DefaultMetricsSystem.shutdown();
    }

    public void incrQuerySuccessCount() {
        querySuccessCount.incr();
    }

    public void incrQueryFailCount() {
        queryFailCount.incr();
    }

    public void incrQueryCount() {
        queryCount.incr();
    }

    public void addQueryLatency(long latency) {
        queryLatency.add(latency);
        for (MutableQuantiles m : queryLatencyTimeMillisQuantiles) {
            m.add(latency);
        }
    }

    public void addScanRowCount(long count) {
        scanRowCount.add(count);
        for (MutableQuantiles m : scanRowCountQuantiles) {
            m.add(count);
        }
    }

    public void addResultRowCount(long count) {
        resultRowCount.add(count);
        for (MutableQuantiles m : resultRowCountQuantiles) {
            m.add(count);
        }
    }

    public void addCacheHitCount(long count) {
        cacheHitCount.incr(count);
        for (MutableQuantiles m : cacheHitCountQuantiles) {
            m.add(count);
        }
    }

    public QueryMetrics registerWith(String name) {
        return DefaultMetricsSystem.instance().register(name, "Query", this);
    }
}
