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

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.MetadataType;
import org.apache.kylin.common.persistence.RawResource;
import org.apache.kylin.common.persistence.transaction.EventListenerRegistry;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.model.NTableMetadataManager;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.metadata.streaming.KafkaConfigManager;
import org.apache.kylin.metadata.table.InternalTableManager;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CacheCleanListener implements EventListenerRegistry.ResourceEventListener {

    @Override
    public void onUpdate(KylinConfig config, RawResource rawResource) {
        // Do nothing. Cache will update internally because mvcc is changed.
    }

    @Override
    public void onDelete(KylinConfig config, String resPath) {
        try {
            Pair<MetadataType, String> typeAndResourceName = MetadataType.splitKeyWithType(resPath);
            String resourceName = typeAndResourceName.getSecond();
            String project;
            switch (typeAndResourceName.getFirst()) {
            case PROJECT:
                project = resourceName;
                if (StringUtils.isNotBlank(project)) {
                    NProjectManager.getInstance(config).invalidCache(project);
                }
                break;
            case TABLE_INFO:
            case TABLE_EXD:
                project = extractProject(resourceName);
                if (StringUtils.isNotBlank(project)) {
                    NTableMetadataManager.getInstance(config, project).invalidCache(resourceName);
                }
                break;
            case KAFKA_CONFIG:
                project = extractProject(resourceName);
                if (StringUtils.isNotBlank(project)) {
                    KafkaConfigManager.getInstance(config, project).invalidCache(resourceName);
                }
                break;
            case INTERNAL_TABLE:
                project = extractProject(resourceName);
                if (StringUtils.isNotBlank(project)) {
                    InternalTableManager.getInstance(config, project).invalidCache(resourceName);
                }
                break;
            default:
            }
        } catch (Exception e) {
            log.error("Unexpected error happened! Clean resource {} cache failed.", resPath, e);
        }

    }

    private String extractProject(String resourceName) {
        return resourceName.substring(0, resourceName.indexOf("."));
    }
}
