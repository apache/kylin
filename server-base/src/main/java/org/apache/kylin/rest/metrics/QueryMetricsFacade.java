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

import java.nio.charset.Charset;
import java.util.Locale;
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.concurrent.ThreadSafe;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.metrics2.MetricsException;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metrics.QuerySparkMetrics;
import org.apache.kylin.rest.request.SQLRequest;
import org.apache.kylin.rest.response.SQLResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kylin.shaded.com.google.common.hash.HashFunction;
import org.apache.kylin.shaded.com.google.common.hash.Hashing;
import org.springframework.security.core.context.SecurityContextHolder;

/**
 * The entrance of metrics features.
 */
@ThreadSafe
public class QueryMetricsFacade {

    private static final Logger logger = LoggerFactory.getLogger(QueryMetricsFacade.class);
    private static final HashFunction hashFunc = Hashing.murmur3_128();

    private static boolean enabled = false;
    private static ConcurrentHashMap<String, QueryMetrics> metricsMap = new ConcurrentHashMap<>();

    public static void init() {
        enabled = KylinConfig.getInstanceFromEnv().getQueryMetricsEnabled();
        if (!enabled)
            return;

        DefaultMetricsSystem.initialize("Kylin");
    }

    private static long getSqlHashCode(String sql) {
        return hashFunc.hashString(sql, Charset.forName("UTF-8")).asLong();
    }

    public static void updateMetrics(String queryId, SQLRequest sqlRequest, SQLResponse sqlResponse) {
        updateMetricsToLocal(sqlRequest, sqlResponse);
        updateMetricsToCache(queryId, sqlRequest, sqlResponse);
    }

    private static void updateMetricsToLocal(SQLRequest sqlRequest, SQLResponse sqlResponse) {
        if (!enabled)
            return;

        String projectName = sqlRequest.getProject();
        update(getQueryMetrics("Server_Total"), sqlResponse);
        update(getQueryMetrics(projectName), sqlResponse);

        String cube = sqlResponse.getCube();
        if (StringUtils.isEmpty(cube)) {
            return;
        }
        String cubeName = cube.replace("=", "->");
        String cubeMetricName = projectName + ",sub=" + cubeName;
        update(getQueryMetrics(cubeMetricName), sqlResponse);
    }

    private static void updateMetricsToCache(String queryId, SQLRequest sqlRequest, SQLResponse sqlResponse) {
        String user = SecurityContextHolder.getContext().getAuthentication().getName();
        if (user == null) {
            user = "unknown";
        }

        QuerySparkMetrics.QueryExecutionMetrics queryExecutionMetrics = QuerySparkMetrics.getInstance()
                .getQueryExecutionMetricsMap().getIfPresent(queryId);
        if (queryExecutionMetrics != null) {
            queryExecutionMetrics.setUser(user);
            queryExecutionMetrics.setSqlIdCode(getSqlHashCode(sqlRequest.getSql()));
            queryExecutionMetrics.setProject(norm(sqlRequest.getProject()));
            queryExecutionMetrics.setQueryType(sqlResponse.isStorageCacheUsed() ? "CACHE" : "PARQUET");
            queryExecutionMetrics.setRealization(sqlResponse.getCube());
            queryExecutionMetrics.setRealizationTypes(sqlResponse.getRealizationTypes());
            queryExecutionMetrics.setCuboidIds(sqlResponse.getCuboidIds());
            queryExecutionMetrics.setSqlDuration(sqlResponse.getDuration());
            queryExecutionMetrics.setTotalScanCount(sqlResponse.getTotalScanCount());
            queryExecutionMetrics.setTotalScanBytes(sqlResponse.getTotalScanBytes());
            queryExecutionMetrics.setResultCount(sqlResponse.getResults() == null ? 0 : sqlResponse.getResults().size());

            queryExecutionMetrics.setException(sqlResponse.getThrowable() == null ? "NULL" :
                    sqlResponse.getThrowable().getClass().getName());
        }
    }

    private static String norm(String project) {
        return project.toUpperCase(Locale.ROOT);
    }

    private static void update(QueryMetrics queryMetrics, SQLResponse sqlResponse) {
        try {
            incrQueryCount(queryMetrics, sqlResponse);
            incrCacheHitCount(queryMetrics, sqlResponse);

            if (!sqlResponse.getIsException()) {
                queryMetrics.addQueryLatency(sqlResponse.getDuration());
                queryMetrics.addScanRowCount(sqlResponse.getTotalScanCount());
                queryMetrics.addResultRowCount(sqlResponse.getResults().size());
            }
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
    }

    private static void incrQueryCount(QueryMetrics queryMetrics, SQLResponse sqlResponse) {
        if (!sqlResponse.isHitExceptionCache() && !sqlResponse.getIsException()) {
            queryMetrics.incrQuerySuccessCount();
        } else {
            queryMetrics.incrQueryFailCount();
        }
        queryMetrics.incrQueryCount();
    }

    private static void incrCacheHitCount(QueryMetrics queryMetrics, SQLResponse sqlResponse) {
        if (sqlResponse.isStorageCacheUsed()) {
            queryMetrics.addCacheHitCount(1);
        }
    }

    private static QueryMetrics getQueryMetrics(String name) {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        int[] intervals = config.getQueryMetricsPercentilesIntervals();

        QueryMetrics queryMetrics = metricsMap.get(name);
        if (queryMetrics != null) {
            return queryMetrics;
        }

        synchronized (QueryMetricsFacade.class) {
            queryMetrics = metricsMap.get(name);
            if (queryMetrics != null) {
                return queryMetrics;
            }

            try {
                queryMetrics = new QueryMetrics(intervals).registerWith(name);
                metricsMap.put(name, queryMetrics);
                return queryMetrics;
            } catch (MetricsException e) {
                logger.warn(name + " register error: ", e);
            }
        }
        return queryMetrics;
    }
}
