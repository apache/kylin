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

package org.apache.kylin.engine.spark.utils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedHashSet;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.StorageURL;
import org.apache.kylin.common.persistence.AutoDeleteDirectory;
import org.apache.kylin.common.persistence.RawResource;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.ResourceTool;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.metadata.TableMetadataManager;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetaDumpUtil {
    private static final Logger logger = LoggerFactory.getLogger(MetaDumpUtil.class);

    public static Set<String> collectCubeMetadata(CubeInstance cube) {
        // cube, model_desc, cube_desc, table
        TableMetadataManager tableMgr = TableMetadataManager.getInstance(cube.getConfig());
        Set<String> dumpList = new LinkedHashSet<>();
        dumpList.add(cube.getResourcePath());
        dumpList.add(cube.getDescriptor().getModel().getResourcePath());
        dumpList.add(cube.getDescriptor().getResourcePath());
        dumpList.add(cube.getProjectInstance().getResourcePath());

        for (TableRef tableRef : cube.getDescriptor().getModel().getAllTables()) {
            TableDesc table = tableRef.getTableDesc();
            dumpList.add(table.getResourcePath());
            dumpList.add(tableMgr.getTableExt(table).getResourcePath());
        }

        return dumpList;
    }

    public static void dumpAndUploadKylinPropsAndMetadata(Set<String> dumpList, KylinConfig kylinConfig,
            String metadataUrl) throws IOException {

        try (AutoDeleteDirectory tmpDir = new AutoDeleteDirectory("kylin_job_meta", "");
                AutoDeleteDirectory metaDir = tmpDir.child("meta")) {
            // dump metadata
            dumpResources(kylinConfig, metaDir.getFile().getAbsolutePath(), dumpList);

            // write kylin.properties
            Properties props = kylinConfig.exportToProperties();
            props.setProperty("kylin.metadata.url", metadataUrl);
            File kylinPropsFile = new File(metaDir.getFile(), "kylin.properties");
            try (FileOutputStream os = new FileOutputStream(kylinPropsFile)) {
                props.store(os, kylinPropsFile.getAbsolutePath());
            }

            KylinConfig dstConfig = KylinConfig.createKylinConfig(props);
            // upload metadata
            new ResourceTool().copy(KylinConfig.createInstanceFromUri(metaDir.getAbsolutePath()), dstConfig);
        }
    }

    public static void dumpResources(KylinConfig kylinConfig, String metaOutDir, Set<String> dumpList)
            throws IOException {
        long startTime = System.currentTimeMillis();

        ResourceStore from = ResourceStore.getStore(kylinConfig);
        KylinConfig localConfig = KylinConfig.createInstanceFromUri(metaOutDir);
        ResourceStore to = ResourceStore.getStore(localConfig);
        final String[] tolerantResources = { "/table_exd" };

        for (String path : dumpList) {
            RawResource res = from.getResource(path);
            if (res == null) {
                if (StringUtils.startsWithAny(path, tolerantResources)) {
                    continue;
                } else {
                    throw new IllegalStateException("No resource found at -- " + path);
                }
            }
            to.putResource(path, res.content(), res.lastModified());
            res.content().close();
        }

        logger.debug("Dump resources to {} took {} ms", metaOutDir, System.currentTimeMillis() - startTime);
    }

    public static KylinConfig loadKylinConfigFromHdfs(String uri) {
        if (uri == null)
            throw new IllegalArgumentException("StorageUrl should not be null");
        if (!uri.contains("@hdfs"))
            throw new IllegalArgumentException("StorageUrl should like @hdfs schema");
        logger.info("Ready to load KylinConfig from uri: {}", uri);
        StorageURL url = StorageURL.valueOf(uri);
        String metaDir = url.getParameter("path") + "/" + KylinConfig.KYLIN_CONF_PROPERTIES_FILE;
        Path path = new Path(metaDir);
        try (InputStream is = path.getFileSystem(HadoopUtil.getCurrentConfiguration()).open(new Path(metaDir))) {
            Properties prop = KylinConfig.streamToProps(is);
            return KylinConfig.createKylinConfig(prop);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
