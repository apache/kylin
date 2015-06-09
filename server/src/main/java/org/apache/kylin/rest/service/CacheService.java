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

package org.apache.kylin.rest.service;

import com.google.common.base.Preconditions;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.cache.CacheUpdater;
import org.apache.kylin.common.restclient.AbstractRestCache;
import org.apache.kylin.common.restclient.Broadcaster;
import org.apache.kylin.cube.CubeDescManager;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.dict.DictionaryManager;
import org.apache.kylin.invertedindex.IIDescManager;
import org.apache.kylin.invertedindex.IIManager;
import org.apache.kylin.job.cube.CubingJob;
import org.apache.kylin.job.cube.CubingJobBuilder;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.metadata.MetadataManager;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.project.ProjectManager;
import org.apache.kylin.rest.constant.Constant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.IOException;

/**
 */
@Component("cacheService")
public class CacheService extends BasicService {

    @Autowired
    private CacheUpdater cacheUpdater;

    @PostConstruct
    public void init() throws IOException {
        initCacheUpdater(cacheUpdater);
    }

    public void initCacheUpdater(CacheUpdater cacheUpdater) {
        Preconditions.checkNotNull(cacheUpdater, "cacheManager is not injected yet");
        AbstractRestCache.setCacheUpdater(cacheUpdater);
    }

    private static final Logger logger = LoggerFactory.getLogger(CacheService.class);

    public void rebuildCache(Broadcaster.TYPE cacheType, String cacheKey) {
        final String log = "rebuild cache type: " + cacheType + " name:" + cacheKey;
        try {
            switch (cacheType) {
            case CUBE:
                CubeInstance newCube = getCubeManager().reloadCubeLocal(cacheKey);
                getProjectManager().clearL2Cache();
                //clean query related cache first
                super.cleanDataCache(newCube.getUuid());
                //move this logic to other place
                mergeCubeOnNewSegmentReady(cacheKey);
                break;
            case CUBE_DESC:
                getCubeDescManager().reloadCubeDescLocal(cacheKey);
                break;
            case PROJECT:
                ProjectInstance projectInstance = getProjectManager().reloadProjectLocal(cacheKey);
                removeOLAPDataSource(projectInstance.getName());
                break;
            case INVERTED_INDEX:
                //II update does not need to update storage cache because it is dynamic already
                getIIManager().reloadIILocal(cacheKey);
                getProjectManager().clearL2Cache();
                break;
            case INVERTED_INDEX_DESC:
                getIIDescManager().reloadIIDescLocal(cacheKey);
                break;
            case TABLE:
                getMetadataManager().reloadTableCache(cacheKey);
                IIDescManager.clearCache();
                CubeDescManager.clearCache();
                break;
            case DATA_MODEL:
                getMetadataManager().reloadDataModelDesc(cacheKey);
                IIDescManager.clearCache();
                CubeDescManager.clearCache();
                break;
            case ALL:
                MetadataManager.clearCache();
                DictionaryManager.clearCache();
                CubeDescManager.clearCache();
                CubeManager.clearCache();
                IIDescManager.clearCache();
                IIManager.clearCache();
                ProjectManager.clearCache();
                cleanAllDataCache();
                removeAllOLAPDataSources();
                break;
            default:
                throw new RuntimeException("invalid cacheType:" + cacheType);
            }
        } catch (IOException e) {
            throw new RuntimeException("error " + log, e);
        }
    }

    public void removeCache(Broadcaster.TYPE cacheType, String cacheKey) {
        final String log = "remove cache type: " + cacheType + " name:" + cacheKey;
        try {
            switch (cacheType) {
            case CUBE:
                String storageUUID = getCubeManager().getCube(cacheKey).getUuid();
                getCubeManager().removeCubeLocal(cacheKey);
                super.cleanDataCache(storageUUID);
                break;
            case CUBE_DESC:
                getCubeDescManager().removeLocalCubeDesc(cacheKey);
                break;
            case PROJECT:
                ProjectManager.clearCache();
                break;
            case INVERTED_INDEX:
                getIIManager().removeIILocal(cacheKey);
                break;
            case INVERTED_INDEX_DESC:
                getIIDescManager().removeIIDescLocal(cacheKey);
                break;
            case TABLE:
                throw new UnsupportedOperationException(log);
            case DATA_MODEL:
                throw new UnsupportedOperationException(log);
            default:
                throw new RuntimeException("invalid cacheType:" + cacheType);
            }
        } catch (IOException e) {
            throw new RuntimeException("error " + log, e);
        }
    }

    private void mergeCubeOnNewSegmentReady(String cubeName) {

        logger.debug("on mergeCubeOnNewSegmentReady: " + cubeName);
        final KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        String serverMode = kylinConfig.getServerMode();

        logger.debug("server mode: " + serverMode);
        if (Constant.SERVER_MODE_JOB.equals(serverMode.toLowerCase()) || Constant.SERVER_MODE_ALL.equals(serverMode.toLowerCase())) {
            logger.debug("This is the job engine node, will check whether auto merge is needed on cube " + cubeName);
            CubeSegment newSeg = null;
            synchronized (CacheService.class) {
                CubeInstance cube = getCubeManager().getCube(cubeName);
                try {
                    newSeg = getCubeManager().autoMergeCubeSegments(cube);
                    if (newSeg != null) {
                        newSeg = getCubeManager().mergeSegments(cube, newSeg.getDateRangeStart(), newSeg.getDateRangeEnd());
                        logger.debug("Will submit merge job on " + newSeg);
                        CubingJobBuilder builder = new CubingJobBuilder(new JobEngineConfig(getConfig()));
                        builder.setSubmitter("SYSTEM");
                        CubingJob job = builder.mergeJob(newSeg);
                        getExecutableManager().addJob(job);
                    } else {
                        logger.debug("Not ready for merge on cube " + cubeName);
                    }

                } catch (IOException e) {
                    logger.error("Failed to auto merge cube " + cubeName, e);
                }
            }
        }
    }
}
