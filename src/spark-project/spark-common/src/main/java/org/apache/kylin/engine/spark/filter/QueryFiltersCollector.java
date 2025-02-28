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

package org.apache.kylin.engine.spark.filter;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.AddressUtil;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.NamedThreadFactory;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryFiltersCollector {

    public static final Logger LOGGER = LoggerFactory.getLogger(QueryFiltersCollector.class);

    // To reduce HDFS storage, only use map to record it.
    // schema: <project, <modelId, <columnId, filter_hit_number>>
    public static final ConcurrentMap<String, Map<String, Map<String, Integer>>> currentQueryFilters = Maps
            .newConcurrentMap();

    public static final ScheduledExecutorService executor = Executors
            .newSingleThreadScheduledExecutor(new NamedThreadFactory("query-filter-collector"));

    public static final String SERVER_HOST = AddressUtil.getLocalServerInfo();

    // path should start with `_` to avoid being cleaned in storage
    public static final String FILTER_STORAGE_PATH = KylinConfig.getInstanceFromEnv().getHdfsWorkingDirectory()
            + "/_query_filter/";

    public static void increaseHit(String project, String modelId, String columnId) {
        if (!KylinConfig.getInstanceFromEnv().isBloomCollectFilterEnabled()) {
            return;
        }
        project = StringUtils.upperCase(project);
        Map<String, Integer> modelFilters = getModelFilters(project, modelId);
        int hit = modelFilters.getOrDefault(columnId, 0);
        modelFilters.put(columnId, ++hit);
    }

    public static void initScheduler() {
        if (!KylinConfig.getInstanceFromEnv().isBloomCollectFilterEnabled()) {
            return;
        }
        executor.scheduleAtFixedRate(() -> {
            long startTime = System.currentTimeMillis();
            LOGGER.info("Start sync query filters, current query filters is " + currentQueryFilters);
            try {
                KylinConfig config = KylinConfig.getInstanceFromEnv();
                FileSystem fs = HadoopUtil.getFileSystem(config.getHdfsWorkingDirectory());
                currentQueryFilters.forEach((project, currentFilters) -> {
                    try {
                        Path projectFilterPath = getProjectFiltersFile(SERVER_HOST, project);
                        Map<String, Map<String, Integer>> mergedHistory;
                        if (!fs.exists(projectFilterPath)) {
                            mergedHistory = currentFilters;
                        } else {
                            mergedHistory = mergeHistory(fs, currentFilters, projectFilterPath);
                        }
                        HadoopUtil.writeStringToHdfs(fs, JsonUtil.writeValueAsString(mergedHistory), projectFilterPath);
                        currentQueryFilters.remove(project);
                    } catch (IOException e) {
                        LOGGER.error("Error when sync query filters for project : " + project, e);
                    }
                });
                long endTime = System.currentTimeMillis();
                LOGGER.info("Sync query filters success. cost time " + (endTime - startTime) + " ms."
                        + " the failed filters maybe " + currentQueryFilters);
            } catch (Throwable e) {
                LOGGER.error("Error when sync query filters...", e);
            }
        }, 0, KylinConfig.getInstanceFromEnv().getQueryFilterCollectInterval(), TimeUnit.SECONDS);
    }

    private static Map<String, Map<String, Integer>> mergeHistory(FileSystem fs,
            Map<String, Map<String, Integer>> currentFilters, Path projectFilterPath) throws IOException {
        Map<String, Map<String, Integer>> history = JsonUtil
                .readValue(HadoopUtil.readStringFromHdfs(fs, projectFilterPath), Map.class);
        currentFilters.forEach((currentModel, currentColumns) -> {
            if (!history.containsKey(currentModel)) {
                history.put(currentModel, currentColumns);
            } else {
                Map<String, Integer> historyColumns = history.get(currentModel);
                currentColumns.forEach((column, hit) -> {
                    Integer oriHit = historyColumns.getOrDefault(column, 0);
                    if (oriHit < 0) {
                        // hit number > Integer.MAX_VALUE, almost impossible to get here
                        oriHit = Integer.MAX_VALUE / 2;
                    }
                    historyColumns.put(column, oriHit + hit);
                });
            }
        });
        return history;
    }

    public static void destoryScheduler() {
        executor.shutdown();
    }

    // schema:  <modelId, <columnId, filter_hit_number>>
    private static Map<String, Map<String, Integer>> getProjectFilters(String project) {
        project = StringUtils.upperCase(project);
        currentQueryFilters.computeIfAbsent(project, key -> Maps.newConcurrentMap());
        return currentQueryFilters.get(project);
    }

    // schema: <columnId, filter_hit_number>
    private static Map<String, Integer> getModelFilters(String project, String modelId) {
        project = StringUtils.upperCase(project);
        Map<String, Map<String, Integer>> projectFilters = getProjectFilters(project);
        projectFilters.computeIfAbsent(modelId, key -> Maps.newConcurrentMap());
        return projectFilters.get(modelId);
    }

    public static Path getProjectFiltersFile(String host, String project) {
        project = StringUtils.upperCase(project);
        return new Path(FILTER_STORAGE_PATH + host + "/" + project);
    }

    private QueryFiltersCollector() {

    }
}
