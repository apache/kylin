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

package org.apache.kylin.cache.memcached;

import static org.apache.kylin.metrics.lib.impl.MetricsSystem.Metrics;

import java.util.Map;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import org.apache.kylin.shaded.com.google.common.collect.Maps;

import net.spy.memcached.metrics.AbstractMetricCollector;
import net.spy.memcached.metrics.DefaultMetricCollector;
import net.spy.memcached.metrics.MetricCollector;

/**
 * A {@link MetricCollector} that uses the Codahale Metrics library.
 *
 * The following system properies can be used to customize the behavior
 * of the collector during runtime:
 */
public final class MemcachedMetrics extends AbstractMetricCollector {

    /**
     * Contains all registered {@link Counter}s.
     */
    private Map<String, Counter> counters;

    /**
     * Contains all registered {@link Meter}s.
     */
    private Map<String, Meter> meters;

    /**
     * Contains all registered {@link Histogram}s.
     */
    private Map<String, Histogram> histograms;

    /**
     * Create a new {@link DefaultMetricCollector}.
     *
     * Note that when this constructor is called, the reporter is also
     * automatically established.
     */
    public MemcachedMetrics() {
        counters = Maps.newConcurrentMap();
        meters = Maps.newConcurrentMap();
        histograms = Maps.newConcurrentMap();
    }

    @Override
    public void addCounter(String name) {
        if (!counters.containsKey(name)) {
            counters.put(name, Metrics.counter(name));
        }
    }

    @Override
    public void removeCounter(String name) {
        if (!counters.containsKey(name)) {
            Metrics.remove(name);
            counters.remove(name);
        }
    }

    @Override
    public void incrementCounter(String name, int amount) {
        if (counters.containsKey(name)) {
            counters.get(name).inc(amount);
        }
    }

    @Override
    public void decrementCounter(String name, int amount) {
        if (counters.containsKey(name)) {
            counters.get(name).dec(amount);
        }
    }

    @Override
    public void addMeter(String name) {
        if (!meters.containsKey(name)) {
            meters.put(name, Metrics.meter(name));
        }
    }

    @Override
    public void removeMeter(String name) {
        if (meters.containsKey(name)) {
            meters.remove(name);
        }
    }

    @Override
    public void markMeter(String name) {
        if (meters.containsKey(name)) {
            meters.get(name).mark();
        }
    }

    @Override
    public void addHistogram(String name) {
        if (!histograms.containsKey(name)) {
            histograms.put(name, Metrics.histogram(name));
        }
    }

    @Override
    public void removeHistogram(String name) {
        if (histograms.containsKey(name)) {
            histograms.remove(name);
        }
    }

    @Override
    public void updateHistogram(String name, int amount) {
        if (histograms.containsKey(name)) {
            histograms.get(name).update(amount);
        }
    }
}