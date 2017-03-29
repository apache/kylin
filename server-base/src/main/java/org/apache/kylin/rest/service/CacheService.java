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

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import javax.sql.DataSource;

import org.apache.calcite.jdbc.Driver;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.metadata.cachesync.Broadcaster;
import org.apache.kylin.metadata.cachesync.Broadcaster.Event;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.query.schema.OLAPSchemaFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.stereotype.Component;

import net.sf.ehcache.CacheManager;

/**
 */
@Component("cacheService")
public class CacheService extends BasicService {

    private static final Logger logger = LoggerFactory.getLogger(CacheService.class);

    private ConcurrentMap<String, DataSource> olapDataSources = new ConcurrentHashMap<String, DataSource>();

    @Autowired
    private CubeService cubeService;

    @Autowired
    private CacheManager cacheManager;

    private Broadcaster.Listener cacheSyncListener = new Broadcaster.Listener() {
        @Override
        public void onClearAll(Broadcaster broadcaster) throws IOException {
            removeAllOLAPDataSources();
            cleanAllDataCache();
        }

        @Override
        public void onProjectSchemaChange(Broadcaster broadcaster, String project) throws IOException {
            removeOLAPDataSource(project);
            cleanDataCache(project);
        }

        @Override
        public void onProjectDataChange(Broadcaster broadcaster, String project) throws IOException {
            removeOLAPDataSource(project); // data availability (cube enabled/disabled) affects exposed schema to SQL
            cleanDataCache(project);
        }

        @Override
        public void onEntityChange(Broadcaster broadcaster, String entity, Event event, String cacheKey) throws IOException {
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

    public void annouceWipeCache(String entity, String event, String cacheKey) {
        Broadcaster broadcaster = Broadcaster.getInstance(getConfig());
        broadcaster.queue(entity, event, cacheKey);
    }

    public void notifyMetadataChange(String entity, Event event, String cacheKey) throws IOException {
        Broadcaster broadcaster = Broadcaster.getInstance(getConfig());

        // broadcaster can be clearCache() too, make sure listener is registered; re-registration will be ignored
        broadcaster.registerListener(cacheSyncListener, "cube");

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

    private void removeOLAPDataSource(String project) {
        logger.info("removeOLAPDataSource is called for project " + project);
        if (StringUtils.isEmpty(project))
            throw new IllegalArgumentException("removeOLAPDataSource: project name not given");

        project = ProjectInstance.getNormalizedProjectName(project);
        olapDataSources.remove(project);
    }

    public void removeAllOLAPDataSources() {
        // brutal, yet simplest way
        logger.info("removeAllOLAPDataSources is called.");
        olapDataSources.clear();
    }

    public DataSource getOLAPDataSource(String project) {

        project = ProjectInstance.getNormalizedProjectName(project);

        DataSource ret = olapDataSources.get(project);
        if (ret == null) {
            logger.debug("Creating a new data source, OLAP data source pointing to " + getConfig());
            File modelJson = OLAPSchemaFactory.createTempOLAPJson(project, getConfig());

            try {
                String text = FileUtils.readFileToString(modelJson, Charset.defaultCharset());
                logger.debug("The new temp olap json is :" + text);
            } catch (IOException e) {
                e.printStackTrace(); // logging failure is not critical
            }

            DriverManagerDataSource ds = new DriverManagerDataSource();
            ds.setDriverClassName(Driver.class.getName());
            ds.setUrl("jdbc:calcite:model=" + modelJson.getAbsolutePath());

            ret = olapDataSources.putIfAbsent(project, ds);
            if (ret == null) {
                ret = ds;
            }
        }
        return ret;
    }

}
