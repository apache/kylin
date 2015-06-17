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

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import net.sf.ehcache.CacheManager;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.CubeDescManager;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.invertedindex.IIDescManager;
import org.apache.kylin.invertedindex.IIManager;
import org.apache.kylin.job.cube.CubingJob;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.Output;
import org.apache.kylin.job.manager.ExecutableManager;
import org.apache.kylin.metadata.MetadataManager;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.project.ProjectManager;
import org.apache.kylin.metadata.realization.RealizationType;
import org.apache.kylin.query.enumerator.OLAPQuery;
import org.apache.kylin.query.relnode.OLAPContext;
import org.apache.kylin.query.schema.OLAPSchemaFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import javax.sql.DataSource;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public abstract class BasicService {

    private static final Logger logger = LoggerFactory.getLogger(BasicService.class);

    private static ConcurrentMap<String, DataSource> olapDataSources = new ConcurrentHashMap<String, DataSource>();

    @Autowired
    private CacheManager cacheManager;

    //    @Autowired
    //    protected JdbcTemplate jdbcTemplate;

    public KylinConfig getConfig() {
        return KylinConfig.getInstanceFromEnv();
    }

    protected void cleanDataCache(String storageUUID) {
        if (cacheManager != null && cacheManager.getCache(storageUUID) != null) {
            cacheManager.getCache(storageUUID).removeAll();
        }
    }

    protected void cleanAllDataCache() {
        if (cacheManager != null)
            cacheManager.clearAll();
    }

    public void removeOLAPDataSource(String project) {
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
            logger.debug("Creating a new data source");
            logger.debug("OLAP data source pointing to " + getConfig());

            File modelJson = OLAPSchemaFactory.createTempOLAPJson(project, getConfig());

            try {
                List<String> text = Files.readLines(modelJson, Charset.defaultCharset());
                logger.debug("The new temp olap json is :");
                for (String line : text)
                    logger.debug(line);
            } catch (IOException e) {
                e.printStackTrace(); // logging failure is not critical
            }

            DriverManagerDataSource ds = new DriverManagerDataSource();
            Properties props = new Properties();
            props.setProperty(OLAPQuery.PROP_SCAN_THRESHOLD, String.valueOf(KylinConfig.getInstanceFromEnv().getScanThreshold()));
            ds.setConnectionProperties(props);
            ds.setDriverClassName("net.hydromatic.optiq.jdbc.Driver");
            ds.setUrl("jdbc:calcite:model=" + modelJson.getAbsolutePath());

            ret = olapDataSources.putIfAbsent(project, ds);
            if (ret == null) {
                ret = ds;
            }
        }
        return ret;
    }

    public final KylinConfig getKylinConfig() {
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();

        if (kylinConfig == null) {
            throw new IllegalArgumentException("Failed to load kylin config instance");
        }

        return kylinConfig;
    }

    public final MetadataManager getMetadataManager() {
        return MetadataManager.getInstance(getConfig());
    }

    public final CubeManager getCubeManager() {
        return CubeManager.getInstance(getConfig());
    }

    public final CubeDescManager getCubeDescManager() {
        return CubeDescManager.getInstance(getConfig());
    }

    public final ProjectManager getProjectManager() {
        return ProjectManager.getInstance(getConfig());
    }

    public final ExecutableManager getExecutableManager() {
        return ExecutableManager.getInstance(getConfig());
    }

    public final IIDescManager getIIDescManager() {
        return IIDescManager.getInstance(getConfig());
    }

    public final IIManager getIIManager() {
        return IIManager.getInstance(getConfig());
    }

    protected List<CubingJob> listAllCubingJobs(final String cubeName, final String projectName, final Set<ExecutableState> statusList, final Map<String, Output> allOutputs) {
        List<CubingJob> results = Lists.newArrayList(FluentIterable.from(getExecutableManager().getAllExecutables()).filter(new Predicate<AbstractExecutable>() {
            @Override
            public boolean apply(AbstractExecutable executable) {
                if (executable instanceof CubingJob) {
                    if (cubeName == null) {
                        return true;
                    }
                    return ((CubingJob) executable).getCubeName().equalsIgnoreCase(cubeName);
                } else {
                    return false;
                }
            }
        }).transform(new Function<AbstractExecutable, CubingJob>() {
            @Override
            public CubingJob apply(AbstractExecutable executable) {
                return (CubingJob) executable;
            }
        }).filter(new Predicate<CubingJob>() {
            @Override
            public boolean apply(CubingJob executable) {
                if (null == projectName || null == getProjectManager().getProject(projectName)) {
                    return true;
                } else {
                    ProjectInstance project = getProjectManager().getProject(projectName);
                    return project.containsRealization(RealizationType.CUBE, executable.getCubeName());
                }
            }
        }).filter(new Predicate<CubingJob>() {
            @Override
            public boolean apply(CubingJob executable) {
                return statusList.contains(allOutputs.get(executable.getId()).getState());
            }
        }));
        return results;
    }

    protected List<CubingJob> listAllCubingJobs(final String cubeName, final String projectName, final Set<ExecutableState> statusList) {
        return listAllCubingJobs(cubeName, projectName, statusList, getExecutableManager().getAllOutputs());
    }

    protected List<CubingJob> listAllCubingJobs(final String cubeName, final String projectName) {
        return listAllCubingJobs(cubeName, projectName, EnumSet.allOf(ExecutableState.class), getExecutableManager().getAllOutputs());
    }

    protected static void close(ResultSet resultSet, Statement stat, Connection conn) {
        OLAPContext.clearParameter();

        if (resultSet != null)
            try {
                resultSet.close();
            } catch (SQLException e) {
                logger.error("failed to close", e);
            }
        if (stat != null)
            try {
                stat.close();
            } catch (SQLException e) {
                logger.error("failed to close", e);
            }
        if (conn != null)
            try {
                conn.close();
            } catch (SQLException e) {
                logger.error("failed to close", e);
            }
    }

}
