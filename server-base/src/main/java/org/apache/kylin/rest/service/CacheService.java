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

import org.apache.kylin.metadata.cachesync.Broadcaster;
import org.apache.kylin.metadata.cachesync.Broadcaster.Event;
import org.apache.kylin.storage.hbase.HBaseConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import net.sf.ehcache.CacheManager;

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

    protected void cleanDataCache(String project) {
        if (cacheManager != null) {
            logger.info("cleaning cache for project " + project + " (currently remove all entries)");
            cacheManager.getCache(QueryService.SUCCESS_QUERY_CACHE).removeAll();
            cacheManager.getCache(QueryService.EXCEPTION_QUERY_CACHE).removeAll();
        } else {
            logger.warn("skip cleaning cache for project " + project);
        }
    }

    protected void cleanAllDataCache() {
        if (cacheManager != null) {
            logger.warn("cleaning all storage cache");
            cacheManager.clearAll();
        } else {
            logger.warn("skip cleaning all storage cache");
        }
    }

}
