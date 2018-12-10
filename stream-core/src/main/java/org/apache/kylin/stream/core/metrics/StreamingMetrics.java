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

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;

public class StreamingMetrics {
    public static final String CONSUME_RATE_PFX = "streaming.events.consume.cnt";
    private static StreamingMetrics instance = new StreamingMetrics();
    private final MetricRegistry metrics = new MetricRegistry();

    private StreamingMetrics() {
    }

    public static StreamingMetrics getInstance() {
        return instance;
    }

    public static Meter newMeter(String name) {
        MetricRegistry metrics = getInstance().getMetrics();
        return metrics.meter(name);
    }

    public MetricRegistry getMetrics() {
        return metrics;
    }

    public void start() {
        //        ConsoleReporter reporter = ConsoleReporter.forRegistry(metrics).convertRatesTo(TimeUnit.SECONDS).convertDurationsTo(TimeUnit.MILLISECONDS)
        //                .build();
        //        reporter.start(5, TimeUnit.SECONDS);
    }

}
