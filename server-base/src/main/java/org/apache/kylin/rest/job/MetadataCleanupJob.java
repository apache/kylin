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
import java.util.Collections;
import java.util.List;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.lang3.ArrayUtils;
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
        FileSystem fs = HadoopUtil.getWorkingFileSystem(HadoopUtil.getCurrentConfiguration());

        List<String> toDeleteCandidates = Lists.newArrayList();

        // two level resources, snapshot tables and cube statistics
        for (String resourceRoot : new String[] { ResourceStore.SNAPSHOT_RESOURCE_ROOT,
                ResourceStore.CUBE_STATISTICS_ROOT, ResourceStore.EXT_SNAPSHOT_RESOURCE_ROOT}) {
            for (String dir : noNull(store.listResources(resourceRoot))) {
                for (String res : noNull(store.listResources(dir))) {
                    if (store.getResourceTimestamp(res) < newResourceTimeCut)
                        toDeleteCandidates.add(res);
                }
            }
        }

        // find all of the global dictionaries in HDFS
        try {
            FileStatus[] fStatus = new FileStatus[0];
            fStatus = ArrayUtils.addAll(fStatus, fs.listStatus(new Path(KylinConfig.getInstanceFromEnv().getHdfsWorkingDirectory() + "resources/GlobalDict/dict")));
            fStatus = ArrayUtils.addAll(fStatus, fs.listStatus(new Path(KylinConfig.getInstanceFromEnv().getHdfsWorkingDirectory() + "resources/SegmentDict/dict")));
            for (FileStatus status : fStatus) {
                String path = status.getPath().toString();
                FileStatus[] globalDicts = fs.listStatus(new Path(path));
                for (FileStatus globalDict : globalDicts) {
                    String globalDictPath = globalDict.getPath().toString();
                    toDeleteCandidates.add(globalDictPath);
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
                        if (store.getResourceTimestamp(res) < newResourceTimeCut)
                            toDeleteCandidates.add(res);
                    }
                }
            }
        }

        // exclude resources in use
        Set<String> activeResources = Sets.newHashSet();
        for (CubeInstance cube : cubeManager.listAllCubes()) {
            activeResources.addAll(cube.getSnapshots().values());
            for (CubeSegment segment : cube.getSegments()) {
                activeResources.addAll(segment.getSnapshotPaths());
                activeResources.addAll(segment.getDictionaryPaths());
                activeResources.add(segment.getStatisticsResourcePath());
                for (String dictPath : segment.getDictionaryPaths()) {
                    DictionaryInfo dictInfo = store.getResource(dictPath, DictionaryInfoSerializer.FULL_SERIALIZER);
                    if ("org.apache.kylin.dict.AppendTrieDictionary".equals(dictInfo != null ? dictInfo.getDictionaryClass() : null)){
                        String dictObj = dictInfo.getDictionaryObject().toString();
                        String basedir = dictObj.substring(dictObj.indexOf("(") + 1, dictObj.indexOf(")") - 1);
                        if (basedir.startsWith(KylinConfig.getInstanceFromEnv().getHdfsWorkingDirectory() + "/resources/GlobalDict")) {
                            activeResources.add(KylinConfig.getInstanceFromEnv().getHdfsWorkingDirectory()
                                    + "resources/GlobalDict" + dictInfo.getResourceDir());
                        } else if (basedir.startsWith(KylinConfig.getInstanceFromEnv().getHdfsWorkingDirectory() + "/resources/SegmentDict")) {
                            activeResources.add(KylinConfig.getInstanceFromEnv().getHdfsWorkingDirectory()
                                    + "resources/SegmentDict" + dictInfo.getResourceDir());
                        }
                    }
                }
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

    private List<String> cleanupConclude(boolean delete, List<String> toDeleteResources) throws IOException {
        if (toDeleteResources.isEmpty()) {
            logger.info("No metadata resource to clean up");
            return toDeleteResources;
        }
        
        logger.info(toDeleteResources.size() + " metadata resource to clean up");

        if (delete) {
            ResourceStore store = ResourceStore.getStore(config);
            FileSystem fs = HadoopUtil.getWorkingFileSystem(HadoopUtil.getCurrentConfiguration());
            for (String res : toDeleteResources) {
                logger.info("Deleting metadata " + res);
                try {
                    if (res.startsWith(KylinConfig.getInstanceFromEnv().getHdfsWorkingDirectory())) {
                        fs.delete(new Path(res), true);
                    } else {
                        store.deleteResource(res);
                    }
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
