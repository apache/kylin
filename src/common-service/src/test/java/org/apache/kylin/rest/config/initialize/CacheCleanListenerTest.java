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

package org.apache.kylin.rest.config.initialize;

import java.util.List;

import org.apache.kylin.cache.utils.ReflectionUtil;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.guava30.shaded.common.cache.Cache;
import org.apache.kylin.junit.annotation.MetadataInfo;
import org.apache.kylin.metadata.cachesync.CachedCrudAssist;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.model.NTableMetadataManager;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.streaming.KafkaConfig;
import org.apache.kylin.metadata.streaming.KafkaConfigManager;
import org.apache.kylin.metadata.table.InternalTableDesc;
import org.apache.kylin.metadata.table.InternalTableManager;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

@MetadataInfo
public class CacheCleanListenerTest {

    static final String PROJECT = "default";

    @Test
    void testOnDelete() {
        CacheCleanListener listener = new CacheCleanListener();
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        NProjectManager projectManager = NProjectManager.getInstance(config);
        NDataModelManager modelManager = NDataModelManager.getInstance(config, PROJECT);
        NTableMetadataManager tableManager = NTableMetadataManager.getInstance(config, PROJECT);
        InternalTableManager internalTableManager = InternalTableManager.getInstance(config, PROJECT);
        KafkaConfigManager kfkManager = KafkaConfigManager.getInstance(config, PROJECT);

        internalTableManager.createInternalTable(new InternalTableDesc());
        KafkaConfig kfkConfig = new KafkaConfig();
        kfkConfig.setName("demo_kfk");
        kfkManager.createKafkaConfig(kfkConfig);

        List<KafkaConfig> confs = kfkManager.listAllKafkaConfigs();
        long cacheSize = getCacheSize(kfkManager, "crud");
        listener.onDelete(config, confs.get(0).getResourcePath());
        Assertions.assertEquals(cacheSize - 1, getCacheSize(kfkManager, "crud"));

        List<NDataModel> models = modelManager.listAllModels();
        cacheSize = getCacheSize(modelManager, "crud");
        listener.onDelete(config, models.get(0).getResourcePath());
        // no need to delete model's cache
        Assertions.assertEquals(cacheSize, getCacheSize(modelManager, "crud"));

        List<InternalTableDesc> internalTables = internalTableManager.listAllTables();
        cacheSize = getCacheSize(internalTableManager, "internalTableCrud");
        listener.onDelete(config, internalTables.get(0).getResourcePath());
        Assertions.assertEquals(cacheSize - 1, getCacheSize(internalTableManager, "internalTableCrud"));

        List<TableDesc> tables = tableManager.listAllTables();
        cacheSize = getCacheSize(tableManager, "srcTableCrud");
        listener.onDelete(config, tables.get(0).getResourcePath());
        Assertions.assertEquals(cacheSize - 1, getCacheSize(tableManager, "srcTableCrud"));

        List<ProjectInstance> projects = projectManager.listAllProjects();
        cacheSize = getCacheSize(projectManager, "crud");
        listener.onDelete(config, projects.get(0).getResourcePath());
        Assertions.assertEquals(cacheSize - 1, getCacheSize(projectManager, "crud"));
    }

    private long getCacheSize(Object manager, String cacheFiled) {
        CachedCrudAssist crud = (CachedCrudAssist) ReflectionUtil.getFieldValue(manager, cacheFiled);
        Cache cache = (Cache) ReflectionUtil.getFieldValue(crud, "cache");
        return cache.size();
    }
}
