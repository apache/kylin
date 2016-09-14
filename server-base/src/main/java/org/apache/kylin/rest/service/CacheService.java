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
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import javax.annotation.PostConstruct;
import javax.sql.DataSource;

import org.apache.calcite.jdbc.Driver;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.restclient.Broadcaster;
import org.apache.kylin.cube.CubeDescManager;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.dict.DictionaryManager;
import org.apache.kylin.engine.streaming.StreamingManager;
import org.apache.kylin.metadata.MetadataManager;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.project.ProjectManager;
import org.apache.kylin.metadata.realization.RealizationRegistry;
import org.apache.kylin.metadata.realization.RealizationType;
import org.apache.kylin.query.enumerator.OLAPQuery;
import org.apache.kylin.query.schema.OLAPSchemaFactory;
import org.apache.kylin.rest.controller.QueryController;
import org.apache.kylin.source.kafka.KafkaConfigManager;
import org.apache.kylin.storage.hbase.HBaseConnection;
import org.apache.kylin.storage.hybrid.HybridManager;
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

    private static ConcurrentMap<String, DataSource> olapDataSources = new ConcurrentHashMap<String, DataSource>();

    @Autowired
    private CubeService cubeService;

    @Autowired
    private CacheManager cacheManager;

    @PostConstruct
    public void initCubeChangeListener() throws IOException {
        CubeManager cubeMgr = CubeManager.getInstance(getConfig());
        cubeMgr.setCubeChangeListener(new CubeManager.CubeChangeListener() {

            @Override
            public void afterCubeCreate(CubeInstance cube) {
                // no cache need change
            }

            @Override
            public void afterCubeUpdate(CubeInstance cube) {
                rebuildCubeCache(cube.getName());
            }

            @Override
            public void afterCubeDelete(CubeInstance cube) {
                removeCubeCache(cube.getName(), cube);
            }
        });
    }

    // for test
    public void setCubeService(CubeService cubeService) {
        this.cubeService = cubeService;
    }

    protected void cleanDataCache(String storageUUID) {
        if (cacheManager != null) {
            logger.info("cleaning cache for " + storageUUID + " (currently remove all entries)");
            cacheManager.getCache(QueryController.SUCCESS_QUERY_CACHE).removeAll();
            cacheManager.getCache(QueryController.EXCEPTION_QUERY_CACHE).removeAll();
        } else {
            logger.warn("skip cleaning cache for " + storageUUID);
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

    private static void removeOLAPDataSource(String project) {
        logger.info("removeOLAPDataSource is called for project " + project);
        if (StringUtils.isEmpty(project))
            throw new IllegalArgumentException("removeOLAPDataSource: project name not given");

        project = ProjectInstance.getNormalizedProjectName(project);
        olapDataSources.remove(project);
    }

    public static void removeAllOLAPDataSources() {
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
            Properties props = new Properties();
            props.setProperty(OLAPQuery.PROP_SCAN_THRESHOLD, String.valueOf(KylinConfig.getInstanceFromEnv().getScanThreshold()));
            ds.setConnectionProperties(props);
            ds.setDriverClassName(Driver.class.getName());
            ds.setUrl("jdbc:calcite:model=" + modelJson.getAbsolutePath());

            ret = olapDataSources.putIfAbsent(project, ds);
            if (ret == null) {
                ret = ds;
            }
        }
        return ret;
    }

    public void rebuildCache(Broadcaster.TYPE cacheType, String cacheKey) {
        final String log = "rebuild cache type: " + cacheType + " name:" + cacheKey;
        logger.info(log);
        try {
            switch (cacheType) {
            case CUBE:
                rebuildCubeCache(cacheKey);
                break;
            case STREAMING:
                getStreamingManager().reloadStreamingConfigLocal(cacheKey);
                break;
            case KAFKA:
                getKafkaManager().reloadKafkaConfigLocal(cacheKey);
                break;
            case CUBE_DESC:
                getCubeDescManager().reloadCubeDescLocal(cacheKey);
                break;
            case PROJECT:
                reloadProjectCache(cacheKey);
                break;
            case TABLE:
                getMetadataManager().reloadTableCache(cacheKey);
                CubeDescManager.clearCache();
                break;
            case EXTERNAL_FILTER:
                getMetadataManager().reloadExtFilter(cacheKey);
                CubeDescManager.clearCache();
                break;
            case DATA_MODEL:
                getMetadataManager().reloadDataModelDesc(cacheKey);
                CubeDescManager.clearCache();
                break;
            case ALL:
                DictionaryManager.clearCache();
                MetadataManager.clearCache();
                CubeDescManager.clearCache();
                CubeManager.clearCache();
                HybridManager.clearCache();
                RealizationRegistry.clearCache();
                Cuboid.clearCache();
                ProjectManager.clearCache();
                KafkaConfigManager.clearCache();
                StreamingManager.clearCache();
                HBaseConnection.clearConnCache();

                cleanAllDataCache();
                removeAllOLAPDataSources();
                break;
            default:
                logger.error("invalid cacheType:" + cacheType);
            }
        } catch (IOException e) {
            throw new RuntimeException("error " + log, e);
        }
    }

    private void rebuildCubeCache(String cubeName) {
        CubeInstance cube = getCubeManager().reloadCubeLocal(cubeName);
        getHybridManager().reloadHybridInstanceByChild(RealizationType.CUBE, cubeName);
        reloadProjectCache(getProjectManager().findProjects(RealizationType.CUBE, cubeName));
        //clean query related cache first
        if (cube != null) {
            cleanDataCache(cube.getUuid());
        }
        cubeService.updateOnNewSegmentReady(cubeName);
    }

    public void removeCache(Broadcaster.TYPE cacheType, String cacheKey) {
        final String log = "remove cache type: " + cacheType + " name:" + cacheKey;
        try {
            switch (cacheType) {
            case CUBE:
                removeCubeCache(cacheKey, null);
                break;
            case CUBE_DESC:
                getCubeDescManager().removeLocalCubeDesc(cacheKey);
                break;
            case PROJECT:
                ProjectManager.clearCache();
                break;
            case TABLE:
                throw new UnsupportedOperationException(log);
            case EXTERNAL_FILTER:
                throw new UnsupportedOperationException(log);
            case DATA_MODEL:
                getMetadataManager().removeModelCache(cacheKey);
                break;
            default:
                throw new RuntimeException("invalid cacheType:" + cacheType);
            }
        } catch (IOException e) {
            throw new RuntimeException("error " + log, e);
        }
    }

    private void removeCubeCache(String cubeName, CubeInstance cube) {
        // you may not get the cube instance if it's already removed from metadata
        if (cube == null) {
            cube = getCubeManager().getCube(cubeName);
        }

        getCubeManager().removeCubeLocal(cubeName);
        getHybridManager().reloadHybridInstanceByChild(RealizationType.CUBE, cubeName);
        reloadProjectCache(getProjectManager().findProjects(RealizationType.CUBE, cubeName));

        if (cube != null) {
            cleanDataCache(cube.getUuid());
        }
    }

    private void reloadProjectCache(List<ProjectInstance> projects) {
        for (ProjectInstance prj : projects) {
            reloadProjectCache(prj.getName());
        }
    }

    private void reloadProjectCache(String projectName) {
        try {
            getProjectManager().reloadProjectLocal(projectName);
        } catch (IOException ex) {
            logger.warn("Failed to reset project cache", ex);
        }
        removeOLAPDataSource(projectName);
    }

}
