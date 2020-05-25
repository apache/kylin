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

package org.apache.kylin.common.tracer;

import java.util.concurrent.ConcurrentMap;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.shaded.com.google.common.collect.Maps;

import io.jaegertracing.Configuration;
import io.jaegertracing.internal.samplers.ConstSampler;

public class TracerManager {

    private static final ConcurrentMap<String, TracerWrapper> CACHE = Maps.newConcurrentMap();

    public static TracerWrapper getTracerWrapper(String serviceName) {
        String endpoint = KylinConfig.getInstanceFromEnv().getTracerCollectorEndpoint();

        TracerWrapper tracerWrapper = CACHE.get(serviceName);
        if (tracerWrapper != null) {
            return tracerWrapper;
        }

        synchronized (TracerManager.class) {
            tracerWrapper = CACHE.get(serviceName);
            if (tracerWrapper != null) {
                return tracerWrapper;
            }
            Configuration.SamplerConfiguration samplerConfig = Configuration.SamplerConfiguration.fromEnv()
                    .withType(ConstSampler.TYPE).withParam(1);
            Configuration.SenderConfiguration sender = Configuration.SenderConfiguration.fromEnv()
                    .withEndpoint(endpoint);
            Configuration.ReporterConfiguration reporterConfig = Configuration.ReporterConfiguration.fromEnv()
                    .withLogSpans(false).withSender(sender);
            Configuration config = new Configuration(serviceName).withSampler(samplerConfig)
                    .withReporter(reporterConfig);
            tracerWrapper = new JaegerTracerWrapper(config.getTracer());
            CACHE.put(serviceName, tracerWrapper);
            return tracerWrapper;
        }
    }
}
