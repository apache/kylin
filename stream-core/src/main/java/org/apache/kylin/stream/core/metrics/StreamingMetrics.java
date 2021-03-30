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

package org.apache.kylin.stream.core.metrics;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.CsvReporter;
import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.jvm.CachedThreadStatesGaugeSet;
import com.codahale.metrics.jvm.GarbageCollectorMetricSet;
import com.codahale.metrics.jvm.MemoryUsageGaugeSet;
import org.apache.kylin.common.KylinConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.concurrent.TimeUnit;

/**
 * https://metrics.dropwizard.io/4.0.0/
 */
public class StreamingMetrics {
    public static final String CONSUME_RATE_PFX = "streaming.events.consume.cnt";
    private static Logger logger = LoggerFactory.getLogger(StreamingMetrics.class);

    private static final String METRICS_OPTION = KylinConfig.getInstanceFromEnv().getStreamMetrics();
    private static final long STREAM_METRICS_INTERVAL = KylinConfig.getInstanceFromEnv().getStreamMetricsInterval();
    private final MetricRegistry metricRegistry = new MetricRegistry();
    private static StreamingMetrics instance = new StreamingMetrics();

    /**
     * ThreadStatesGaugeSet & ClassLoadingGaugeSet are currently not registered.
     * metricRegistry.register("threads", new ThreadStatesGaugeSet());
     */
    private StreamingMetrics() {
        if (METRICS_OPTION != null && !METRICS_OPTION.isEmpty()) {
            metricRegistry.register("gc", new GarbageCollectorMetricSet());
            metricRegistry.register("threads", new CachedThreadStatesGaugeSet(10, TimeUnit.SECONDS));
            metricRegistry.register("memory", new MemoryUsageGaugeSet());
        }
    }

    public static StreamingMetrics getInstance() {
        return instance;
    }

    public static Meter newMeter(String name) {
        MetricRegistry metrics = getInstance().getMetricRegistry();
        return metrics.meter(name);
    }

    public MetricRegistry getMetricRegistry() {
        return metricRegistry;
    }

    public void start() {
        switch (METRICS_OPTION) {
        case "":
            logger.info("Skip streaming metricRegistry because it is empty.");
            break;
        // for test purpose
        case "console":
            logger.info("Use console to collect streaming metricRegistry.");
            ConsoleReporter consoleReporter = ConsoleReporter.forRegistry(metricRegistry)
                    .convertRatesTo(TimeUnit.SECONDS).convertDurationsTo(TimeUnit.MILLISECONDS).build();
            consoleReporter.start(STREAM_METRICS_INTERVAL, TimeUnit.SECONDS);
            break;
        case "csv":
            File metricsFolder = new File("stream_metrics_csv");
            if (!metricsFolder.exists()) {
                boolean res = metricsFolder.mkdirs();
                if (!res) {
                    logger.error("Cannot create dir for stream_metrics_csv");
                    break;
                }
            }
            logger.info("Collect streaming metricRegistry in csv format.");
            CsvReporter scvReporter = CsvReporter.forRegistry(metricRegistry).convertRatesTo(TimeUnit.SECONDS)
                    .convertDurationsTo(TimeUnit.MILLISECONDS).build(metricsFolder);
            scvReporter.start(STREAM_METRICS_INTERVAL, TimeUnit.SECONDS);
            break;
        case "jmx":
            final JmxReporter jmxReporter = JmxReporter.forRegistry(metricRegistry).build();
            jmxReporter.start();
            break;
        default:
            logger.info("Skip metricRegistry because the option {} is not identified.", METRICS_OPTION);
        }
    }

}
