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

package org.apache.kylin.engine.mr.common;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.RawResource;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.source.SourceManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.LinkedHashSet;
import java.util.Set;

public class JobRelatedMetaUtil {
    private static final Logger logger = LoggerFactory.getLogger(JobRelatedMetaUtil.class);

    public static Set<String> collectCubeMetadata(CubeInstance cube) {
        // cube, model_desc, cube_desc, table
        Set<String> dumpList = new LinkedHashSet<>();
        dumpList.add(cube.getResourcePath());
        dumpList.add(cube.getDescriptor().getModel().getResourcePath());
        dumpList.add(cube.getDescriptor().getResourcePath());
        dumpList.add(cube.getProjectInstance().getResourcePath());

        for (TableRef tableRef : cube.getDescriptor().getModel().getAllTables()) {
            TableDesc table = tableRef.getTableDesc();
            dumpList.add(table.getResourcePath());
            dumpList.addAll(SourceManager.getMRDependentResources(table));
        }

        return dumpList;
    }

    public static void dumpResources(KylinConfig kylinConfig, File metaDir, Set<String> dumpList) throws IOException {
        long startTime = System.currentTimeMillis();

        ResourceStore from = ResourceStore.getStore(kylinConfig);
        KylinConfig localConfig = KylinConfig.createInstanceFromUri(metaDir.getAbsolutePath());
        ResourceStore to = ResourceStore.getStore(localConfig);
        for (String path : dumpList) {
            RawResource res = from.getResource(path);
            if (res == null)
                throw new IllegalStateException("No resource found at -- " + path);
            to.putResource(path, res.inputStream, res.timestamp);
            res.inputStream.close();
        }

        logger.debug("Dump resources to {} took {} ms", metaDir, System.currentTimeMillis() - startTime);
    }
}
