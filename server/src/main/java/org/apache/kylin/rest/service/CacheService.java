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

import org.apache.kylin.common.restclient.Broadcaster;
import org.apache.kylin.cube.CubeDescManager;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.invertedindex.IIDescManager;
import org.apache.kylin.invertedindex.IIManager;
import org.apache.kylin.metadata.MetadataManager;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.project.ProjectManager;
import org.apache.kylin.metadata.realization.RealizationRegistry;
import org.apache.kylin.metadata.realization.RealizationType;
import org.apache.kylin.storage.hybrid.HybridManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.List;

/**
 * Created by qianzhou on 1/19/15.
 */
@Component("cacheService")
public class CacheService extends BasicService {

    @Autowired
    private CubeService cubeService;
    
    public void rebuildCache(Broadcaster.TYPE cacheType, String cacheKey) {
        final String log = "rebuild cache type: " + cacheType + " name:" + cacheKey;
        try {
            switch (cacheType) {
            case CUBE:
                getCubeManager().loadCubeCache(cacheKey);
                cubeService.updateOnNewSegmentReady(cacheKey);
                getHybridManager().reloadHybridInstanceByChild(RealizationType.CUBE, cacheKey);
                cleanProjectCacheByRealization(RealizationType.CUBE, cacheKey);
                break;
            case CUBE_DESC:
                getCubeDescManager().reloadCubeDesc(cacheKey);
                break;
            case PROJECT:
                getProjectManager().reloadProject(cacheKey);
                break;
            case INVERTED_INDEX:
                getIIManager().loadIICache(cacheKey);
                getHybridManager().reloadHybridInstanceByChild(RealizationType.INVERTED_INDEX, cacheKey);
                cleanProjectCacheByRealization(RealizationType.INVERTED_INDEX, cacheKey);
                break;
            case INVERTED_INDEX_DESC:
                getIIDescManager().reloadIIDesc(cacheKey);
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
                CubeDescManager.clearCache();
                CubeManager.clearCache();
                IIDescManager.clearCache();
                IIManager.clearCache();
                HybridManager.clearCache();
                RealizationRegistry.clearCache();
                ProjectManager.clearCache();
                BasicService.resetOLAPDataSources();
                break;
            default:
                throw new RuntimeException("invalid cacheType:" + cacheType);
            }
        } catch (IOException e) {
            throw new RuntimeException("error " + log, e);
        }

    }

    private void cleanProjectCacheByRealization(RealizationType type, String realizationName) throws IOException {
        List<ProjectInstance> projectInstances = getProjectManager().findProjects(type, realizationName);
        for (ProjectInstance pi : projectInstances) {
            getProjectManager().reloadProject(pi.getName());
            removeOLAPDataSource(pi.getName());
        }
    }

    public void removeCache(Broadcaster.TYPE cacheType, String cacheKey) {
        final String log = "remove cache type: " + cacheType + " name:" + cacheKey;
        try {
            switch (cacheType) {
            case CUBE:
                getCubeManager().removeCubeCacheLocal(cacheKey);
                break;
            case CUBE_DESC:
                getCubeDescManager().removeLocalCubeDesc(cacheKey);
                break;
            case PROJECT:
                ProjectManager.clearCache();
                break;
            case INVERTED_INDEX:
                getIIManager().removeIILocalCache(cacheKey);
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
}
