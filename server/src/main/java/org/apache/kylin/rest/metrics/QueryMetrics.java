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

import java.util.HashMap;
import java.util.Map;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricSet;

/**
 * @author xduo
 * 
 */
public class QueryMetrics implements MetricSet {

    private Map<String, Float> metrics = new HashMap<String, Float>();

    private QueryMetrics() {
        // register query metrics
        this.increase("duration", (float) 0);
        this.increase("totalScanCount", (float) 0);
        this.increase("count", (float) 0);
    }

    static class QueryMetricsHolder {
        static final QueryMetrics INSTANCE = new QueryMetrics();
    }

    public static QueryMetrics getInstance() {
        return QueryMetricsHolder.INSTANCE;
    }

    public synchronized void increase(String key, Float value) {
        if (metrics.containsKey(key)) {
            metrics.put(key, metrics.get(key) + value);
        } else {
            metrics.put(key, value);
        }
    }

    public synchronized Float getAndReset(String key) {
        float value = metrics.get(key);
        metrics.put(key, (float) 0);

        return value;
    }

    public synchronized Map<String, Metric> getMetrics() {
        Map<String, Metric> metricSet = new HashMap<String, Metric>();

        for (final String key : metrics.keySet()) {
            metricSet.put(key, new Gauge<Float>() {
                @Override
                public Float getValue() {
                    float value = getAndReset(key);

                    return value;
                }
            });
        }

        return metricSet;
    }
}
