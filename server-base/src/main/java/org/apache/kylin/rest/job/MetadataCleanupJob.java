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
import java.util.Collections;
import java.util.List;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.job.dao.ExecutableDao;
import org.apache.kylin.job.dao.ExecutablePO;
import org.apache.kylin.job.execution.ExecutableState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class MetadataCleanupJob {

    private static final Logger logger = LoggerFactory.getLogger(MetadataCleanupJob.class);

    private static final long NEW_RESOURCE_THREADSHOLD_MS = 12 * 3600 * 1000L; // 12 hour

    // ============================================================================

    final KylinConfig config;
    
    private List<String> garbageResources = Collections.emptyList();
    
    public MetadataCleanupJob() {
        this(KylinConfig.getInstanceFromEnv());
    }
    
    public MetadataCleanupJob(KylinConfig config) {
        this.config = config;
    }
    
    public List<String> getGarbageResources() {
        return garbageResources;
    }

    // function entrance
    public List<String> cleanup(boolean delete, int jobOutdatedDays) throws Exception {
        CubeManager cubeManager = CubeManager.getInstance(config);
        ResourceStore store = ResourceStore.getStore(config);
        long newResourceTimeCut = System.currentTimeMillis() - NEW_RESOURCE_THREADSHOLD_MS;

        List<String> toDeleteCandidates = Lists.newArrayList();

        // two level resources, snapshot tables and cube statistics
        for (String resourceRoot : new String[] { ResourceStore.SNAPSHOT_RESOURCE_ROOT,
                ResourceStore.CUBE_STATISTICS_ROOT }) {
            for (String dir : noNull(store.listResources(resourceRoot))) {
                for (String res : noNull(store.listResources(dir))) {
                    if (store.getResourceTimestamp(res) < newResourceTimeCut)
                        toDeleteCandidates.add(res);
                }
            }
        }

        // three level resources, only dictionaries
        for (String resourceRoot : new String[] { ResourceStore.DICT_RESOURCE_ROOT }) {
            for (String dir : noNull(store.listResources(resourceRoot))) {
                for (String dir2 : noNull(store.listResources(dir))) {
                    for (String res : noNull(store.listResources(dir2))) {
                        if (store.getResourceTimestamp(res) < newResourceTimeCut)
                            toDeleteCandidates.add(res);
                    }
                }
            }
        }

        // exclude resources in use
        Set<String> activeResources = Sets.newHashSet();
        for (CubeInstance cube : cubeManager.listAllCubes()) {
            for (CubeSegment segment : cube.getSegments()) {
                activeResources.addAll(segment.getSnapshotPaths());
                activeResources.addAll(segment.getDictionaryPaths());
                activeResources.add(segment.getStatisticsResourcePath());
            }
        }
        toDeleteCandidates.removeAll(activeResources);

        // delete old and completed jobs
        long outdatedJobTimeCut = System.currentTimeMillis() - jobOutdatedDays * 24 * 3600 * 1000L;
        ExecutableDao executableDao = ExecutableDao.getInstance(config);
        List<ExecutablePO> allExecutable = executableDao.getJobs();
        for (ExecutablePO executable : allExecutable) {
            long lastModified = executable.getLastModified();
            String jobStatus = executableDao.getJobOutput(executable.getUuid()).getStatus();

            if (lastModified < outdatedJobTimeCut && (ExecutableState.SUCCEED.toString().equals(jobStatus)
                    || ExecutableState.DISCARDED.toString().equals(jobStatus))) {
                toDeleteCandidates.add(ResourceStore.EXECUTE_RESOURCE_ROOT + "/" + executable.getUuid());
                toDeleteCandidates.add(ResourceStore.EXECUTE_OUTPUT_RESOURCE_ROOT + "/" + executable.getUuid());

                for (ExecutablePO task : executable.getTasks()) {
                    toDeleteCandidates.add(ResourceStore.EXECUTE_OUTPUT_RESOURCE_ROOT + "/" + task.getUuid());
                }
            }
        }
        
        garbageResources = cleanupConclude(delete, toDeleteCandidates);
        return garbageResources;
    }

    private List<String> cleanupConclude(boolean delete, List<String> toDeleteResources) {
        if (toDeleteResources.isEmpty()) {
            logger.info("No metadata resource to clean up");
            return toDeleteResources;
        }
        
        logger.info(toDeleteResources.size() + " metadata resource to clean up");

        if (delete) {
            ResourceStore store = ResourceStore.getStore(config);
            for (String res : toDeleteResources) {
                logger.info("Deleting metadata " + res);
                try {
                    store.deleteResource(res);
                } catch (IOException e) {
                    logger.error("Failed to delete resource " + res, e);
                }
            }
        } else {
            for (String res : toDeleteResources) {
                logger.info("Dry run, pending delete metadata " + res);
            }
        }
        return toDeleteResources;
    }

    private NavigableSet<String> noNull(NavigableSet<String> list) {
        return (list == null) ? new TreeSet<String>() : list;
    }

}
