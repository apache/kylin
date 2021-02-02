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

package org.apache.kylin.rest.job;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.engine.mr.CubingJob;
import org.apache.kylin.job.dao.ExecutableDao;
import org.apache.kylin.job.dao.ExecutableOutputPO;
import org.apache.kylin.job.dao.ExecutablePO;
import org.apache.kylin.job.exception.PersistentException;
import org.apache.kylin.job.execution.CardinalityExecutable;
import org.apache.kylin.job.execution.CheckpointExecutable;
import org.apache.kylin.job.execution.ExecutableState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kylin.shaded.com.google.common.collect.Maps;

public class MetadataCleanupJob {

    private static final Logger logger = LoggerFactory.getLogger(MetadataCleanupJob.class);

    // ============================================================================

    final KylinConfig config;

    private Map<String, Long> garbageResources = Maps.newHashMap();
    private ResourceStore store;

    public MetadataCleanupJob() {
        this(KylinConfig.getInstanceFromEnv());
    }

    public MetadataCleanupJob(KylinConfig config) {
        this.config = config;
        this.store = ResourceStore.getStore(config);
    }

    public Map<String, Long> getGarbageResources() {
        return garbageResources;
    }

    // function entrance
    public Map<String, Long> cleanup(boolean delete, int jobOutdatedDays) throws Exception {
        Map<String, Long> toDeleteCandidates = Maps.newHashMap();

        // delete old and completed jobs
        long outdatedJobTimeCut = System.currentTimeMillis() - jobOutdatedDays * 24 * 3600 * 1000L;
        ExecutableDao executableDao = ExecutableDao.getInstance(config);
        List<ExecutablePO> allExecutable = executableDao.getJobs();
        for (ExecutablePO executable : allExecutable) {
            long lastModified = executable.getLastModified();
            if (lastModified < outdatedJobTimeCut && isJobComplete(executableDao, executable)) {
                String jobResPath = ResourceStore.EXECUTE_RESOURCE_ROOT + "/" + executable.getUuid();
                String jobOutputResPath = ResourceStore.EXECUTE_OUTPUT_RESOURCE_ROOT + "/" + executable.getUuid();
                long outputLastModified = getTimestamp(jobOutputResPath);
                toDeleteCandidates.put(jobResPath, lastModified);
                toDeleteCandidates.put(jobOutputResPath, outputLastModified);

                List<ExecutablePO> tasks = executable.getTasks();
                if (tasks != null && !tasks.isEmpty()) {
                    for (ExecutablePO task : executable.getTasks()) {
                        String taskId = task.getUuid();
                        if (StringUtils.isNotBlank(taskId)) {
                            String resPath = ResourceStore.EXECUTE_OUTPUT_RESOURCE_ROOT + "/" + task.getUuid();
                            long timestamp = getTimestamp(resPath);
                            toDeleteCandidates.put(resPath, timestamp);
                        }
                    }
                }
            }
        }

        garbageResources = cleanupConclude(delete, toDeleteCandidates);
        return garbageResources;
    }

    private boolean isJobComplete(ExecutableDao executableDao, ExecutablePO job) {
        String jobId = job.getUuid();
        boolean isComplete = false;
        try {
            ExecutableOutputPO output = executableDao.getJobOutput(jobId);
            String status = output.getStatus();
            String jobType = job.getType();
            if (jobType.equals(CubingJob.class.getName())
                    || jobType.equals(CheckpointExecutable.class.getName())) {
                if (StringUtils.equals(status, ExecutableState.SUCCEED.toString())
                        || StringUtils.equals(status, ExecutableState.DISCARDED.toString())) {
                    isComplete = true;
                }
            } else if (jobType.equals(CardinalityExecutable.class.getName())) {
                // Ignore state of DefaultChainedExecutable
                isComplete = true;
            }
        } catch (PersistentException e) {
            logger.error("Get job output failed for job uuid: {}", jobId, e);
            isComplete = true; // job output broken --> will be treat as complete
        }

        return isComplete;
    }

    private Map<String, Long> cleanupConclude(boolean delete, Map<String, Long> toDeleteResources) throws IOException {
        if (toDeleteResources.isEmpty()) {
            logger.info("No metadata resource to clean up");
            return toDeleteResources;
        }

        logger.info("{} metadata resource to clean up", toDeleteResources.size());

        if (delete) {
            ResourceStore store = ResourceStore.getStore(config);
            FileSystem fs = HadoopUtil.getWorkingFileSystem(HadoopUtil.getCurrentConfiguration());
            for (String res : toDeleteResources.keySet()) {
                long timestamp = toDeleteResources.get(res);
                logger.info("Deleting metadata=[resource_path: {}, timestamp: {}]", res, timestamp);
                try {
                    if (res.startsWith(KylinConfig.getInstanceFromEnv().getHdfsWorkingDirectory())) {
                        fs.delete(new Path(res), true);
                    } else {
                        store.deleteResource(res, timestamp);
                    }
                } catch (IOException e) {
                    logger.error("Failed to delete metadata=[resource_path: {}, timestamp: {}] ", res, timestamp, e);
                }
            }
        } else {
            for (String res : toDeleteResources.keySet()) {
                long timestamp = toDeleteResources.get(res);
                logger.info("Dry run, pending delete metadata=[resource_path: {}, timestamp: {}] ", res, timestamp);
            }
        }
        return toDeleteResources;
    }

    private long getTimestamp(String resPath) {
        long timestamp = Long.MAX_VALUE;
        try {
            timestamp = store.getResourceTimestamp(resPath);
        } catch (IOException e) {
            logger.warn("Failed to get resource timestamp from remote resource store, details:{}", e);
        }
        return timestamp;
    }
}
