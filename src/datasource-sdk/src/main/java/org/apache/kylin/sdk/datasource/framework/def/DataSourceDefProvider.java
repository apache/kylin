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
package org.apache.kylin.sdk.datasource.framework.def;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.sdk.datasource.framework.utils.XmlUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kylin.guava30.shaded.common.collect.Maps;

public class DataSourceDefProvider {
    private static final Logger logger = LoggerFactory.getLogger(DataSourceDefProvider.class);
    private static final String RESOURCE_DIR = "datasource";
    private static final String DATASOURCE_DEFAULT = "default";

    private static final DataSourceDefProvider INSTANCE = new DataSourceDefProvider();
    private final Map<String, DataSourceDef> dsCache = Maps.newConcurrentMap();
    private final ClassLoader cl = getClass().getClassLoader();

    public static DataSourceDefProvider getInstance() {
        return INSTANCE;
    }

    private DataSourceDef loadDataSourceFromEnv(String id) {
        String resourcePath = RESOURCE_DIR + "/" + id + ".xml";
        String resourcePathOverride = resourcePath + ".override";
        InputStream is = null;
        try {
            URL urlOverride, url;
            urlOverride = cl.getResource(resourcePathOverride);
            if (urlOverride == null) {
                url = cl.getResource(resourcePath);
            } else {
                url = urlOverride;
                logger.debug("Use override xml:{}", resourcePathOverride);
            }
            if (url == null)
                return null;

            is = url.openStream();
            DataSourceDef ds = XmlUtil.readValue(is, DataSourceDef.class);
            ds.init();
            return ds;
        } catch (IOException e) {
            logger.error("Failed to load data source: Path={}", resourcePath, e);
            return null;
        } finally {
            IOUtils.closeQuietly(is);
        }
    }

    // only for dev purpose
    private DataSourceDef loadDataSourceFromDir(String id) {
        String resourcePath = KylinConfig.getKylinHome() + "/conf/datasource/" + id + ".xml";
        try (InputStream is = new FileInputStream(resourcePath)) {
            DataSourceDef ds = XmlUtil.readValue(is, DataSourceDef.class);
            ds.init();
            return ds;
        } catch (IOException e) {
            logger.error("[Dev Only] Failed to load data source from directory.: Path={}", resourcePath,
                    e.getMessage());
            return null;
        }
    }

    private boolean isDevEnv() {
        return (System.getenv("KYLIN_HOME") != null || System.getProperty("KYLIN_HOME") != null)
                && KylinConfig.getInstanceFromEnv().isDevEnv();
    }

    // ===============================================

    public DataSourceDef getById(String id) {
        if (isDevEnv()) {
            DataSourceDef devDs = loadDataSourceFromDir(id);
            if (devDs != null)
                return devDs;
        }

        DataSourceDef ds = dsCache.get(id);
        if (ds != null)
            return ds;

        synchronized (this) {
            ds = dsCache.get(id);
            if (ds == null) {
                ds = loadDataSourceFromEnv(id);
                if (ds != null)
                    dsCache.put(id, ds);
            }
        }
        return ds;
    }

    public DataSourceDef getDefault() {
        return getById(DATASOURCE_DEFAULT);
    }

}
