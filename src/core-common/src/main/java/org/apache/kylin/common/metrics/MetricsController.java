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

package org.apache.kylin.common.metrics;

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.jmx.JmxReporter;

public class MetricsController {
    private static final Logger logger = LoggerFactory.getLogger(MetricsController.class);

    private static final AtomicBoolean reporterStarted = new AtomicBoolean(false);

    private static volatile MetricRegistry defaultMetricRegistry = null;

    private MetricsController() {
    }

    // try register metrics background
    // like: 1. NProjectManager.listAllProjects foreach register counters
    // 2. NDataModelManager.listAllModels register gauge
    // 3...

    public static MetricRegistry getDefaultMetricRegistry() {
        if (defaultMetricRegistry == null) {
            synchronized (MetricsController.class) {
                if (defaultMetricRegistry == null) {
                    defaultMetricRegistry = new MetricRegistry();
                }
            }
        }
        return defaultMetricRegistry;
    }

    public static void startReporters(KapConfig verifiableProps) {
        if (KylinConfig.getInstanceFromEnv().isDevOrUT()) {
            return;
        }
        synchronized (reporterStarted) {
            if (!reporterStarted.get()) {
                try {
                    final MetricsReporter influxDbReporter = MetricsInfluxdbReporter.getInstance();
                    influxDbReporter.init(verifiableProps);

                    final JmxReporter jmxReporter = JmxReporter.forRegistry(getDefaultMetricRegistry()).build();
                    jmxReporter.start();

                    reporterStarted.set(true);

                    logger.info("ke.metrics reporters started");
                } catch (Exception e) {
                    logger.error("ke.metrics reporters start failed", e);
                }
            }
        }
    }

}
