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

package org.apache.kylin.common.metrics.reporter;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.metrics.MetricsName;

import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricFilter;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ServerModeMetricFilter implements MetricFilter {

    private static final String SERVER_MODE;

    static {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        SERVER_MODE = config.getServerMode();
    }

    @Override
    public boolean matches(String name, Metric metric) {
        // TODO Because isLeader is always false, some metric will not be collected. need to be redesigned.
        boolean isLeader = false;
        String[] split = name.split(":");
        if (split.length > 1) {
            String metricName = split[0];
            MetricsName metricsName = MetricsName.getMetricsName(metricName);
            if (metricsName != null) {
                return metricsName.support(SERVER_MODE, isLeader);
            }
        }
        return true;
    }
}
