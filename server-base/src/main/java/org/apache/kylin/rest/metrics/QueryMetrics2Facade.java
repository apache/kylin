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

import static org.apache.kylin.common.metrics.common.MetricsConstant.TOTAL;
import static org.apache.kylin.common.metrics.common.MetricsNameBuilder.buildCubeMetricPrefix;

import java.util.concurrent.TimeUnit;

import javax.annotation.concurrent.ThreadSafe;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.metrics.common.Metrics;
import org.apache.kylin.common.metrics.common.MetricsConstant;
import org.apache.kylin.common.metrics.common.MetricsFactory;
import org.apache.kylin.common.metrics.common.MetricsNameBuilder;
import org.apache.kylin.rest.request.SQLRequest;
import org.apache.kylin.rest.response.SQLResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The entrance of metrics features.
 */
@ThreadSafe
public class QueryMetrics2Facade {

    private static final Logger logger = LoggerFactory.getLogger(QueryMetrics2Facade.class);
    private static Metrics metrics;
    private static boolean enabled = false;

    public static void init() {
        enabled = KylinConfig.getInstanceFromEnv().getQueryMetrics2Enabled();
    }

    public static void updateMetrics(SQLRequest sqlRequest, SQLResponse sqlResponse) {
        if (!enabled) {
            return;
        }
        if (metrics == null) {
            metrics = MetricsFactory.getInstance();
        }
        String projectName = sqlRequest.getProject();
        String cube = sqlResponse.getCube();
        if (StringUtils.isEmpty(cube)) {
            return;
        }
        String cubeName = cube.replace("=", "->");

        //        update(getQueryMetrics("Server_Total"), sqlResponse);
        update(buildCubeMetricPrefix(TOTAL), sqlResponse);
        update(buildCubeMetricPrefix(projectName), sqlResponse);
        String cubeMetricName = buildCubeMetricPrefix(projectName, cubeName);
        update(cubeMetricName, sqlResponse);
    }

    private static void update(String name, SQLResponse sqlResponse) {
        try {
            incrQueryCount(name, sqlResponse);
            incrCacheHitCount(name, sqlResponse);
            if (!sqlResponse.getIsException()) {
                metrics.updateTimer(MetricsNameBuilder.buildMetricName(name, MetricsConstant.QUERY_DURATION),
                        sqlResponse.getDuration(), TimeUnit.MILLISECONDS);
                metrics.updateHistogram(MetricsNameBuilder.buildMetricName(name, MetricsConstant.QUERY_RESULT_ROWCOUNT),
                        sqlResponse.getResults().size());
                metrics.updateHistogram(MetricsNameBuilder.buildMetricName(name, MetricsConstant.QUERY_SCAN_ROWCOUNT),
                        sqlResponse.getTotalScanCount());
            }
        } catch (Exception e) {
            logger.error(e.getMessage());
        }

    }

    private static void incrQueryCount(String name, SQLResponse sqlResponse) {
        if (!sqlResponse.isHitExceptionCache() && !sqlResponse.getIsException()) {
            metrics.incrementCounter(MetricsNameBuilder.buildMetricName(name, MetricsConstant.QUERY_SUCCESS_COUNT));
        } else {
            metrics.incrementCounter(MetricsNameBuilder.buildMetricName(name, MetricsConstant.QUERY_FAIL_COUNT));
        }
        metrics.incrementCounter(MetricsNameBuilder.buildMetricName(name, MetricsConstant.QUERY_COUNT));
    }

    private static void incrCacheHitCount(String name, SQLResponse sqlResponse) {
        if (sqlResponse.isStorageCacheUsed()) {
            metrics.incrementCounter(MetricsNameBuilder.buildMetricName(name, MetricsConstant.QUERY_CACHE_COUNT));
        }
    }

}
