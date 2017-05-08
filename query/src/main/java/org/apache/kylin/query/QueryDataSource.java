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

package org.apache.kylin.query;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import javax.sql.DataSource;

import org.apache.calcite.jdbc.Driver;
import org.apache.commons.dbcp.BasicDataSource;
import org.apache.commons.dbcp.BasicDataSourceFactory;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.query.schema.OLAPSchemaFactory;
import org.apache.log4j.Logger;

import com.google.common.collect.Lists;

public class QueryDataSource {

    private static final Logger logger = Logger.getLogger(QueryDataSource.class);

    private ConcurrentMap<String, DataSource> olapDataSources = new ConcurrentHashMap<String, DataSource>();
    private List<File> usedFiles = Lists.newLinkedList();

    /**
     * Get available data source
     * 
     * @param project
     * @param config
     * @return
     * @throws Exception
     */
    public DataSource get(String project, KylinConfig config) {
        return get(project, config, new Properties());
    }

    /**
     * Get available data source with advanced DBCP configuration
     * 
     * @see http://commons.apache.org/proper/commons-dbcp/configuration.html
     * 
     * @param project
     * @param config
     * @return
     */
    public DataSource get(String project, KylinConfig config, Properties props) {
        if (project == null) {
            throw new IllegalArgumentException("project should not be null");
        }
        
        DataSource ds = olapDataSources.get(project);
        if (ds != null) {
            return ds;
        }
        
        WrappedDataSource wrappedDS = getWrapped(project, config, props);
        ds = wrappedDS.getDataSource();
        olapDataSources.putIfAbsent(project, ds);
        usedFiles.add(wrappedDS.getOlapFile());
        return ds;
    }

    public DataSource removeCache(String project) {
        return olapDataSources.remove(project);
    }

    public void clearCache() {
        olapDataSources.clear();
        for (File usedFile : usedFiles) {
            FileUtils.deleteQuietly(usedFile);
        }
    }

    /**
     * Get data source without cache
     * 
     * @param project
     * @param config
     * @return
     */
    public static DataSource create(String project, KylinConfig config) {
        return create(project, config, new Properties());
    }

    /**
     * Get data source without cache, advanced configuration
     * 
     * @param project
     * @param config
     * @param props
     * @return
     */
    public static DataSource create(String project, KylinConfig config, Properties props) {
        return getWrapped(project, config, props).getDataSource();
    }

    private static WrappedDataSource getWrapped(String project, KylinConfig config, Properties props) {
        File olapTmp = OLAPSchemaFactory.createTempOLAPJson(project, config);
        if (logger.isDebugEnabled()) {
            try {
                String text = FileUtils.readFileToString(olapTmp, Charset.defaultCharset());
                logger.debug("The new temp olap json is :" + text);
            } catch (IOException e) {
                // logging failure is not critical
            }
        }

        BasicDataSource ds = null;
        if (StringUtils.isEmpty(props.getProperty("maxActive"))) {
            props.setProperty("maxActive", "-1");
        }
        try {
            ds = (BasicDataSource) BasicDataSourceFactory.createDataSource(props);
        } catch (Exception e) {
            // Basic mode
            ds = new BasicDataSource();
            ds.setMaxActive(-1);
        }
        ds.setUrl("jdbc:calcite:model=" + olapTmp.getAbsolutePath());
        ds.setDriverClassName(Driver.class.getName());

        WrappedDataSource wrappedDS = new WrappedDataSource(ds, olapTmp);
        return wrappedDS;
    }

    private static class WrappedDataSource {
        private DataSource ds;
        private File tempOlap;

        private WrappedDataSource(DataSource dataSource, File olapModel) {
            this.ds = dataSource;
            this.tempOlap = olapModel;
        }

        public DataSource getDataSource() {
            return ds;
        }

        public File getOlapFile() {
            return tempOlap;
        }
    }
}
