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

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.metadata.EpochStore;
import org.apache.kylin.common.util.AddressUtil;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.NamedThreadFactory;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.metadata.project.ProjectInstance;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * 1. Unify the entry point for all calculation calls to obtain the capacity of the WorkingDir through scheduled threads
 * 2. Expose two configurations externally:
 * - function enable switch: kylin.metrics.hdfs-periodic-calculation-enabled  - default true
 * - polling time parameter: kylin.metrics.hdfs-periodic-calculation-interval - default 5min
 */
@Slf4j
public class HdfsCapacityMetrics {

    protected static final KylinConfig KYLIN_CONFIG;
    protected static final String SERVICE_INFO;
    protected static final Path HDFS_CAPACITY_METRICS_PATH;
    protected static final FileSystem WORKING_FS;
    protected static final ScheduledExecutorService HDFS_METRICS_SCHEDULED_EXECUTOR;
    protected static boolean hdfsMetricsPeriodicCalculationEnabled;
    protected static boolean quotaStorageEnabled;
    // For all places that need to query WorkingDir capacity for retrieval, initialize to avoid NPE
    protected static ConcurrentMap<String, Long> workingDirCapacity = new ConcurrentHashMap<>();
    // Used to clear the existing workingDirCapacity in memory, you cannot use the clear method for workingDirCapacity
    // to avoid other calls to raise NPE, When the data in memory is ready, it is first put into readyWorkingDirCapacity,
    // and then a data exchange operation is performed.
    protected static ConcurrentMap<String, Long> prepareForWorkingDirCapacity = new ConcurrentHashMap<>();

    static {
        KYLIN_CONFIG = KylinConfig.getInstanceFromEnv();
        SERVICE_INFO = AddressUtil.getLocalInstance();
        WORKING_FS = HadoopUtil.getWorkingFileSystem();
        HDFS_CAPACITY_METRICS_PATH = new Path(KYLIN_CONFIG.getHdfsMetricsDir("hdfsCapacity.json"));
        HDFS_METRICS_SCHEDULED_EXECUTOR = Executors.newScheduledThreadPool(1, new NamedThreadFactory("HdfsMetricsChecker"));
        registerHdfsMetrics();
    }

    // Utility classes should not have public constructors
    private HdfsCapacityMetrics() {
    }

    public static void registerHdfsMetrics() {
        // 1. Call a scheduled thread to maintain the data in memory
        //    - Read data from HDFS and load it into memory, only the leader node writes to HDFS, other nodes read only
        // 2. When the data in memory reaches the time of the update interval, it is stored on HDFS
        // 3. Junk cleanup: theoretically the file will not be very large, do not need to consider cleaning up for the time
        // being, cleaning will affect the recalculation of the directory involved
        hdfsMetricsPeriodicCalculationEnabled = KYLIN_CONFIG.isHdfsMetricsPeriodicCalculationEnabled();
        quotaStorageEnabled = KYLIN_CONFIG.isStorageQuotaEnabled();
        if (quotaStorageEnabled && hdfsMetricsPeriodicCalculationEnabled) {
            log.info("Quota storage and HDFS metrics periodic calculation are enabled, path: {}", HDFS_CAPACITY_METRICS_PATH);
            HDFS_METRICS_SCHEDULED_EXECUTOR.scheduleAtFixedRate(HdfsCapacityMetrics::handleNodeHdfsMetrics,
                    0, KYLIN_CONFIG.getHdfsMetricsPeriodicCalculationInterval(), TimeUnit.MILLISECONDS);
        }
    }

    public static void handleNodeHdfsMetrics() {
        // Check whether the current KE node is the leader node, which requires repeated and continuous monitoring
        // because the leader node may change. Update first and then overwrite, only leader nodes need to be overwritten,
        // other nodes are read only
        if (EpochStore.isLeaderNode()) {
            writeHdfsMetrics();
        } else {
            workingDirCapacity = readHdfsMetrics();
        }
    }

    public static void writeHdfsMetrics() {
        prepareForWorkingDirCapacity.clear();
        // All WorkingDir capacities involved are calculated here
        Set<String> allProjects = NProjectManager.getInstance(KYLIN_CONFIG).listAllProjects()
                .stream().map(ProjectInstance::getName).collect(Collectors.toSet());
        try {
            for (String project : allProjects) {
                // Should not initialize projectTotalStorageSize outside the loop, otherwise it may affect the next calculation
                // if a project calculation throws an exception.
                long projectTotalStorageSize = 0L;
                Path projectPath = new Path(KYLIN_CONFIG.getWorkingDirectoryWithConfiguredFs(project));
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
            FSDataOutputStream fsDataOutputStream = WORKING_FS.create(HDFS_CAPACITY_METRICS_PATH, true);
            JsonUtil.writeValue(fsDataOutputStream, workingDirCapacity);
        } catch (IOException e) {
            log.warn("Write HdfsCapacityMetrics failed.", e);
        }
    }

    @SuppressWarnings("unchecked")
    public static ConcurrentMap<String, Long> readHdfsMetrics() {
        ConcurrentHashMap<String, Long> workingCapacity = new ConcurrentHashMap<>();
        try {
            if (WORKING_FS.exists(HDFS_CAPACITY_METRICS_PATH)) {
                FSDataInputStream fsDataInputStream = WORKING_FS.open(HDFS_CAPACITY_METRICS_PATH);
                workingCapacity = JsonUtil.readValue(fsDataInputStream, ConcurrentHashMap.class);
            }
        } catch (IOException e) {
            log.warn("Read HdfsCapacityMetrics failed.", e);
        }
        return workingCapacity;
    }

    /**
     * To use this method, the caller needs to determine whether the return value is -1
     * See {@link org.apache.kylin.metadata.cube.storage.TotalStorageCollector}
     *
     * @return HDFS Capacity by each project
     */
    public static Long getHdfsCapacityByProject(String project) {
        if (hdfsMetricsPeriodicCalculationEnabled) {
            // Writing numbers in JSON may be read as integer
            Object orDefault = workingDirCapacity.getOrDefault(project, 0L);
            return Long.parseLong(orDefault.toString());
        }
        return -1L;
    }
}