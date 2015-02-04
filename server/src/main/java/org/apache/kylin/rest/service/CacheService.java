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
import org.apache.kylin.invertedindex.IIDescManager;
import org.apache.kylin.metadata.project.ProjectManager;
import org.springframework.stereotype.Component;

import java.io.IOException;

/**
 * Created by qianzhou on 1/19/15.
 */
@Component("cacheService")
public class CacheService extends BasicService {

    public void rebuildCache(Broadcaster.TYPE cacheType, String cacheKey) {
        final String log = "rebuild cache type: " + cacheType + " name:" + cacheKey;
        try {
            switch (cacheType) {
                case CUBE:
                    getCubeManager().loadCubeCache(cacheKey);
                    break;
                case CUBE_DESC:
                    getCubeDescManager().reloadCubeDesc(cacheKey);
                    break;
                case PROJECT:
                    getProjectManager().reloadProject(cacheKey);
                    break;
                case INVERTED_INDEX:
                    getIIManager().loadIICache(cacheKey);
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
