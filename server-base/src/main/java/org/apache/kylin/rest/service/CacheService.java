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

import java.io.IOException;
import java.util.Map;

import org.apache.kylin.metadata.cachesync.Broadcaster;
import org.apache.kylin.metadata.cachesync.Broadcaster.Event;
import org.apache.kylin.storage.hbase.HBaseConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.cache.CacheManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;


/**
 */
@Component("cacheService")
public class CacheService extends BasicService implements InitializingBean {

    private static final Logger logger = LoggerFactory.getLogger(CacheService.class);

    @Autowired
    @Qualifier("cubeMgmtService")
    private CubeService cubeService;

    @Autowired
    private CacheManager cacheManager;

    private Broadcaster.Listener cacheSyncListener = new Broadcaster.Listener() {
        @Override
        public void onClearAll(Broadcaster broadcaster) throws IOException {
            cleanAllDataCache();
            HBaseConnection.clearConnCache(); // take the chance to clear HBase connection cache as well
        }

        @Override
        public void onProjectSchemaChange(Broadcaster broadcaster, String project) throws IOException {
            cleanDataCache(project);
        }

        @Override
        public void onProjectDataChange(Broadcaster broadcaster, String project) throws IOException {
            cleanDataCache(project);
        }

        @Override
        public void onProjectQueryACLChange(Broadcaster broadcaster, String project) throws IOException {
            cleanDataCache(project);
        }

        @Override
        public void onEntityChange(Broadcaster broadcaster, String entity, Event event, String cacheKey)
                throws IOException {
            if ("cube".equals(entity) && event == Event.UPDATE) {
                final String cubeName = cacheKey;
                new Thread() { // do not block the event broadcast thread
                    public void run() {
                        try {
                            Thread.sleep(1000);
                            cubeService.updateOnNewSegmentReady(cubeName);
                        } catch (Throwable ex) {
                            logger.error("Error in updateOnNewSegmentReady()", ex);
                        }
                    }
                }.start();
            }
        }
    };

    // for test
    public void setCubeService(CubeService cubeService) {
        this.cubeService = cubeService;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        Broadcaster.getInstance(getConfig()).registerStaticListener(cacheSyncListener, "cube");
    }

    public void wipeProjectCache(String project) {
        if (project == null)
            annouceWipeCache("all", "update", "all");
        else
            annouceWipeCache("project", "update", project);
    }

    public void annouceWipeCache(String entity, String event, String cacheKey) {
        Broadcaster broadcaster = Broadcaster.getInstance(getConfig());
        broadcaster.announce(entity, event, cacheKey);
    }

    public void notifyMetadataChange(String entity, Event event, String cacheKey) throws IOException {
        Broadcaster broadcaster = Broadcaster.getInstance(getConfig());
        broadcaster.notifyListener(entity, event, cacheKey);
    }

    public void cleanDataCache(String project) {
        if (cacheManager != null) {
            if (getConfig().isQueryCacheSignatureEnabled()) {
                logger.info("cleaning cache for project " + project + " (currently remove nothing)");
            } else {
                logger.info("cleaning cache for project " + project + " (currently remove all entries)");
                cacheManager.getCache(QueryService.QUERY_CACHE).clear();
            }
        } else {
            logger.warn("skip cleaning cache for project " + project);
        }
    }

    protected void cleanAllDataCache() {
        if (cacheManager != null) {
            logger.warn("cleaning all storage cache");
            for (String cacheName : cacheManager.getCacheNames()) {
                logger.warn("cleaning storage cache for {}", cacheName);
                cacheManager.getCache(cacheName).clear();
            }
        } else {
            logger.warn("skip cleaning all storage cache");
        }
    }

    public void clearCacheForCubeMigration(String cube, String project, String model, Map<String, String> tableToProjects) throws IOException {
        //the metadata reloading must be in order

        //table must before model
        for (Map.Entry<String, String> entry : tableToProjects.entrySet()) {
            //For KYLIN-2717 compatibility, use tableProject not project
            getTableManager().reloadSourceTableQuietly(entry.getKey(), entry.getValue());
            getTableManager().reloadTableExtQuietly(entry.getKey(), entry.getValue());
        }
        logger.info("reload table cache done");

        //ProjectInstance cache must before cube and model cache, as the new cubeDesc init and model reloading relays on latest ProjectInstance cache
        getProjectManager().reloadProjectQuietly(project);
        logger.info("reload project cache done");

        //model must before cube desc
        getDataModelManager().reloadDataModel(model);
        logger.info("reload model cache done");

        //cube desc must before cube instance
        getCubeDescManager().reloadCubeDescLocal(cube);
        logger.info("reload cubeDesc cache done");

        getCubeManager().reloadCubeQuietly(cube);
        logger.info("reload cube cache done");

        //reload project l2cache again after cube cache, because the project L2 cache relay on latest cube cache
        getProjectManager().reloadProjectL2Cache(project);
        logger.info("reload project l2cache done");
    }

}
