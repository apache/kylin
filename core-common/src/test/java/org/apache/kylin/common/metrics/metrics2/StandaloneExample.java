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

package org.apache.kylin.common.metrics.metrics2;

import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.configuration.SubsetConfiguration;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.sink.FileSink;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.MetricRegistry;

/**
 * Modified from https://github.com/joshelser/dropwizard-hadoop-metrics2, Copyright by Josh Elser
 *
 * A little utility to try to simulate "real-life" scenarios. Doesn't actually assert anything yet
 * so it requires human interaction.
 */
public class StandaloneExample {

    public static void main(String[] args) throws Exception {
        final MetricRegistry metrics = new MetricRegistry();

        final HadoopMetrics2Reporter metrics2Reporter = HadoopMetrics2Reporter.forRegistry(metrics).build(
                DefaultMetricsSystem.initialize("StandaloneTest"), // The application-level name
                "Test", // Component name
                "Test", // Component description
                "Test"); // Name for each metric record
        final ConsoleReporter consoleReporter = ConsoleReporter.forRegistry(metrics).build();

        MetricsSystem metrics2 = DefaultMetricsSystem.instance();
        // Writes to stdout without a filename configuration
        // Will be invoked every 10seconds by default
        FileSink sink = new FileSink();
        metrics2.register("filesink", "filesink", sink);
        sink.init(new SubsetConfiguration(null, null) {
            public String getString(String key) {
                if (key.equals("filename")) {
                    return null;
                }
                return super.getString(key);
            }
        });

        // How often should the dropwizard reporter be invoked
        metrics2Reporter.start(500, TimeUnit.MILLISECONDS);
        // How often will the dropwziard metrics be logged to the console
        consoleReporter.start(2, TimeUnit.SECONDS);

        generateMetrics(metrics, 5000, 25, TimeUnit.MILLISECONDS, metrics2Reporter, 10);
    }

    /**
     * Runs a number of threads which generate metrics.
     */
    public static void generateMetrics(final MetricRegistry metrics, final long metricsToGenerate, final int period,
            final TimeUnit periodTimeUnit, HadoopMetrics2Reporter metrics2Reporter, int numThreads) throws Exception {
        final ScheduledExecutorService pool = Executors.newScheduledThreadPool(numThreads);
        final CountDownLatch latch = new CountDownLatch(numThreads);

        for (int i = 0; i < numThreads; i++) {
            final int id = i;
            final int halfPeriod = (period / 2);
            Runnable task = new Runnable() {
                private long executions = 0;
                final Random r = new Random();

                @Override
                public void run() {
                    if (executions >= metricsToGenerate) {
                        return;
                    }
                    metrics.counter("foo counter thread" + id).inc();
                    executions++;
                    if (executions < metricsToGenerate) {
                        pool.schedule(this, period + r.nextInt(halfPeriod), periodTimeUnit);
                    } else {
                        latch.countDown();
                    }
                }
            };
            pool.schedule(task, period, periodTimeUnit);
        }

        while (!latch.await(2, TimeUnit.SECONDS)) {
            metrics2Reporter.printQueueDebugMessage();
        }

        pool.shutdown();
        pool.awaitTermination(5000, TimeUnit.SECONDS);
    }
}
