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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.dict.DictionaryInfo;
import org.apache.kylin.dict.DictionaryInfoSerializer;
import org.apache.kylin.job.dao.ExecutableDao;
import org.apache.kylin.job.dao.ExecutableOutputPO;
import org.apache.kylin.job.dao.ExecutablePO;
import org.apache.kylin.job.exception.PersistentException;
import org.apache.kylin.job.execution.ExecutableState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class MetadataCleanupJob {

    private static final Logger logger = LoggerFactory.getLogger(MetadataCleanupJob.class);

    private static final long NEW_RESOURCE_THREADSHOLD_MS = 12 * 3600 * 1000L; // 12 hour

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
        CubeManager cubeManager = CubeManager.getInstance(config);
        long newResourceTimeCut = System.currentTimeMillis() - NEW_RESOURCE_THREADSHOLD_MS;
        FileSystem fs = HadoopUtil.getWorkingFileSystem(HadoopUtil.getCurrentConfiguration());

        Map<String, Long> toDeleteCandidates = Maps.newHashMap();

        // two level resources, snapshot tables and cube statistics
        for (String resourceRoot : new String[] { ResourceStore.SNAPSHOT_RESOURCE_ROOT,
                ResourceStore.CUBE_STATISTICS_ROOT, ResourceStore.EXT_SNAPSHOT_RESOURCE_ROOT }) {
            for (String dir : noNull(store.listResources(resourceRoot))) {
                for (String res : noNull(store.listResources(dir))) {
                    long timestamp = getTimestamp(res);
                    if (timestamp < newResourceTimeCut)
                        toDeleteCandidates.put(res, timestamp);
                }
            }
        }

        // find all of the global dictionaries in HDFS
        try {
            FileStatus[] fStatus = new FileStatus[0];
            fStatus = ArrayUtils.addAll(fStatus, fs.listStatus(new Path(
                    KylinConfig.getInstanceFromEnv().getHdfsWorkingDirectory() + "resources/GlobalDict/dict")));
            fStatus = ArrayUtils.addAll(fStatus, fs.listStatus(new Path(
                    KylinConfig.getInstanceFromEnv().getHdfsWorkingDirectory() + "resources/SegmentDict/dict")));
            for (FileStatus status : fStatus) {
                String path = status.getPath().toString();
                FileStatus[] globalDicts = fs.listStatus(new Path(path));
                for (FileStatus globalDict : globalDicts) {
                    String globalDictPath = globalDict.getPath().toString();
                    long timestamp = getTimestamp(globalDict);
                    if (timestamp < newResourceTimeCut)
                        toDeleteCandidates.put(globalDictPath, timestamp);
                }
            }
        } catch (FileNotFoundException e) {
            logger.info("Working Directory does not exist on HDFS. ");
        }

        // three level resources, only dictionaries
        for (String resourceRoot : new String[] { ResourceStore.DICT_RESOURCE_ROOT }) {
            for (String dir : noNull(store.listResources(resourceRoot))) {
                for (String dir2 : noNull(store.listResources(dir))) {
                    for (String res : noNull(store.listResources(dir2))) {
                        long timestamp = getTimestamp(res);
                        if (timestamp < newResourceTimeCut)
                            toDeleteCandidates.put(res, timestamp);
                    }
                }
            }
        }

        // exclude resources in use
        Set<String> activeResources = Sets.newHashSet();
        for (CubeInstance cube : cubeManager.reloadAndListAllCubes()) {
            activeResources.addAll(cube.getSnapshots().values());
            for (CubeSegment segment : cube.getSegments()) {
                activeResources.addAll(segment.getSnapshotPaths());
                activeResources.addAll(segment.getDictionaryPaths());
                activeResources.add(segment.getStatisticsResourcePath());
                for (String dictPath : segment.getDictionaryPaths()) {
                    DictionaryInfo dictInfo = store.getResource(dictPath, DictionaryInfoSerializer.FULL_SERIALIZER);
                    if ("org.apache.kylin.dict.AppendTrieDictionary"
                            .equals(dictInfo != null ? dictInfo.getDictionaryClass() : null)) {
                        String dictObj = dictInfo.getDictionaryObject().toString();
                        String basedir = dictObj.substring(dictObj.indexOf("(") + 1, dictObj.indexOf(")") - 1);
                        if (basedir.startsWith(
                                KylinConfig.getInstanceFromEnv().getHdfsWorkingDirectory() + "/resources/GlobalDict")) {
                            activeResources.add(KylinConfig.getInstanceFromEnv().getHdfsWorkingDirectory()
                                    + "resources/GlobalDict" + dictInfo.getResourceDir());
                        } else if (basedir.startsWith(KylinConfig.getInstanceFromEnv().getHdfsWorkingDirectory()
                                + "/resources/SegmentDict")) {
                            activeResources.add(KylinConfig.getInstanceFromEnv().getHdfsWorkingDirectory()
                                    + "resources/SegmentDict" + dictInfo.getResourceDir());
                        }
                    }
                }
            }
        }
        toDeleteCandidates.keySet().removeAll(activeResources);

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
            if (StringUtils.equals(status, ExecutableState.SUCCEED.toString())
                    || StringUtils.equals(status, ExecutableState.DISCARDED.toString())) {
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

    private NavigableSet<String> noNull(NavigableSet<String> list) {
        return (list == null) ? new TreeSet<>() : list;
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

    private long getTimestamp(FileStatus filestatus) {
        return filestatus.getModificationTime();
    }
}
