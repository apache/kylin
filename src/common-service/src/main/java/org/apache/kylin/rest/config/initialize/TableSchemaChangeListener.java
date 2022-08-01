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

import java.util.Objects;
import java.util.Optional;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.RawResource;
import org.apache.kylin.common.persistence.transaction.EventListenerRegistry;
import org.apache.kylin.rest.service.CommonQueryCacheSupporter;

/**
 * Lister that monitors the table schema chagnes via KE metadata changes on path /{procjet}/table/{table}.json
 * On table schema changes, the listener will
 *    1. invalidate all table schema cache under the same project
 */
public class TableSchemaChangeListener implements EventListenerRegistry.ResourceEventListener {

    private final CommonQueryCacheSupporter queryCacheManager;

    public TableSchemaChangeListener(CommonQueryCacheSupporter queryCacheManager) {
        this.queryCacheManager = queryCacheManager;
    }

    @Override
    public void onUpdate(KylinConfig config, RawResource rawResource) {
        if (Objects.isNull(rawResource)) {
            return;
        }
        getProjectName(rawResource.getResPath()).ifPresent(project -> clearSchemaCache(config, project));
    }

    @Override
    public void onDelete(KylinConfig config, String resPath) {
        getProjectName(resPath).ifPresent(project -> clearSchemaCache(config, project));
    }

    private Optional<String> getProjectName(String resourcePath) {
        if (Objects.isNull(resourcePath)) {
            return Optional.empty();
        }
        if (!resourcePath.contains("table")) {
            return Optional.empty();
        }

        String[] elements = resourcePath.split("/");
        // acl resource path like '/{project}/table/{table}.json
        if (elements.length != 4 || elements[2].equalsIgnoreCase("table") || StringUtils.isEmpty(elements[1])) {
            return Optional.empty();
        }
        return Optional.of(elements[1]);
    }

    private void clearSchemaCache(KylinConfig config, String project) {
        if (!config.isRedisEnabled()) {
            queryCacheManager.onClearSchemaCache(project);
        }
    }
}
