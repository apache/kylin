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

package org.apache.kylin.metrics;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.metadata.EpochStore;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.NamedThreadFactory;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.metadata.project.ProjectInstance;

import com.google.common.collect.Maps;

import lombok.extern.slf4j.Slf4j;

/**
 * 1. Unify the entry point for all calculation calls to obtain the capacity of the WorkingDir through scheduled threads
 * 2. Expose two configurations externally:
 * - function enable switch: kylin.metrics.hdfs-periodic-calculation-enabled  - default true
 * - polling time parameter: kylin.metrics.hdfs-periodic-calculation-interval - default 5min
 */
@Slf4j
public class HdfsCapacityMetrics {

    private final Path hdfsCapacityMetricsPath;
    private final FileSystem workingFs;
    private final ScheduledThreadPoolExecutor scheduledThreadPoolExecutor;
    private final KylinConfig config;
    private final boolean quotaStorageEnabled;
    private final boolean hdfsMetricsPeriodicCalculationEnabled;
    // For all places that need to query WorkingDir capacity for retrieval, initialize to avoid NPE
    private volatile Map<String, Long> workingDirCapacity = Collections.emptyMap();

    // Utility classes should not have public constructors
    public HdfsCapacityMetrics(KylinConfig config) {
        this.config = config;
        workingFs = HadoopUtil.getWorkingFileSystem();
        hdfsCapacityMetricsPath = new Path(config.getHdfsMetricsDir("hdfsCapacity.json"));
        scheduledThreadPoolExecutor = new ScheduledThreadPoolExecutor(1, new NamedThreadFactory("HdfsMetricsChecker"));
        hdfsMetricsPeriodicCalculationEnabled = config.isHdfsMetricsPeriodicCalculationEnabled();
        quotaStorageEnabled = config.isStorageQuotaEnabled();
        if (hdfsMetricsPeriodicCalculationEnabled && quotaStorageEnabled) {
            registerHdfsMetrics(config.getHdfsMetricsPeriodicCalculationInterval());
        }
    }

    public int getPoolSize() {
        return scheduledThreadPoolExecutor.getPoolSize();
    }

    public int getActiveCount() {
        return scheduledThreadPoolExecutor.getActiveCount();
    }

    Map<String, Long> getWorkingDirCapacity() {
        return Collections.unmodifiableMap(workingDirCapacity);
    }

    public Path getHdfsCapacityMetricsPath() {
        return hdfsCapacityMetricsPath;
    }

    private void registerHdfsMetrics(long hdfsMetricsPeriodicCalculationInterval) {
        // 1. Call a scheduled thread to maintain the data in memory
        //    - Read data from HDFS and load it into memory, only the leader node writes to HDFS, other nodes read only
        // 2. When the data in memory reaches the time of the update interval, it is stored on HDFS
        // 3. Junk cleanup: theoretically the file will not be very large, do not need to consider cleaning up for the time
        // being, cleaning will affect the recalculation of the directory involved
        log.info("Quota storage and HDFS metrics periodic calculation are enabled, path: {}", hdfsCapacityMetricsPath);
        scheduledThreadPoolExecutor.scheduleAtFixedRate(this::handleNodeHdfsMetrics, 0,
                hdfsMetricsPeriodicCalculationInterval, TimeUnit.MILLISECONDS);
    }

    public void handleNodeHdfsMetrics() {
        // Check whether the current KE node is the leader node, which requires repeated and continuous monitoring
        // because the leader node may change. Update first and then overwrite, only leader nodes need to be overwritten,
        // other nodes are read only
        if (EpochStore.isLeaderNode()) {
            writeHdfsMetrics();
        } else {
            workingDirCapacity = readHdfsMetrics();
        }
    }

    public void writeHdfsMetrics() {
        // All WorkingDir capacities involved are calculated here
        Set<String> allProjects = NProjectManager.getInstance(config).listAllProjects().stream()
                .map(ProjectInstance::getName).collect(Collectors.toSet());
        HashMap<String, Long> prepareForWorkingDirCapacity = Maps.newHashMapWithExpectedSize(allProjects.size());
        try {
            for (String project : allProjects) {
                // Should not initialize projectTotalStorageSize outside the loop, otherwise it may affect the next calculation
                // if a project calculation throws an exception.
                long projectTotalStorageSize = 0L;
                Path projectPath = new Path(config.getWorkingDirectoryWithConfiguredFs(project));
                FileSystem fs = projectPath.getFileSystem(HadoopUtil.getCurrentConfiguration());
                if (fs.exists(projectPath)) {
                    projectTotalStorageSize = HadoopUtil.getContentSummary(fs, projectPath).getLength();
                }
                prepareForWorkingDirCapacity.put(project, projectTotalStorageSize);
            }
        } catch (IOException e) {
            log.warn("Projects update workingDirCapacity failed.", e);
        }
        // If the project is deleted, it will be updated here
        workingDirCapacity = prepareForWorkingDirCapacity;
        try {
            FSDataOutputStream fsDataOutputStream = workingFs.create(hdfsCapacityMetricsPath, true);
            JsonUtil.writeValue(fsDataOutputStream, workingDirCapacity);
        } catch (IOException e) {
            log.warn("Write HdfsCapacityMetrics failed.", e);
        }
    }

    @SuppressWarnings("unchecked")
    public Map<String, Long> readHdfsMetrics() {
        try {
            if (workingFs.exists(hdfsCapacityMetricsPath)) {
                FSDataInputStream fsDataInputStream = workingFs.open(hdfsCapacityMetricsPath);
                return JsonUtil.readValue(fsDataInputStream, HashMap.class);
            }
        } catch (IOException e) {
            log.warn("Read HdfsCapacityMetrics failed.", e);
        }
        return Collections.emptyMap();
    }

    /**
     * To use this method, the caller needs to determine whether the return value is -1
     * See {@link org.apache.kylin.metadata.cube.storage.TotalStorageCollector}
     *
     * @return HDFS Capacity by each project
     */
    public Long getHdfsCapacityByProject(String project) {
        if (hdfsMetricsPeriodicCalculationEnabled && quotaStorageEnabled) {
            // Writing numbers in JSON may be read as integer
            Object orDefault = workingDirCapacity.getOrDefault(project, 0L);
            return Long.parseLong(orDefault.toString());
        }
        return -1L;
    }
}