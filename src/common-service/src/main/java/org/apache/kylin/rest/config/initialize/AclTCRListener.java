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

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.RawResource;
import org.apache.kylin.common.persistence.transaction.EventListenerRegistry;
import org.apache.kylin.rest.service.CommonQueryCacheSupporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AclTCRListener implements EventListenerRegistry.ResourceEventListener {

    private static final Logger logger = LoggerFactory.getLogger(AclTCRListener.class);

    private final CommonQueryCacheSupporter queryCacheManager;

    public AclTCRListener(CommonQueryCacheSupporter queryCacheManager) {
        this.queryCacheManager = queryCacheManager;
    }

    @Override
    public void onUpdate(KylinConfig config, RawResource rawResource) {
        if (Objects.isNull(rawResource)) {
            return;
        }
        getProjectName(rawResource.getMetaKey()).ifPresent(project -> clearCache(config, project));
    }

    @Override
    public void onDelete(KylinConfig config, String resPath) {
        getProjectName(resPath).ifPresent(project -> clearCache(config, project));
    }

    private Optional<String> getProjectName(String resourcePath) {
        if (Objects.isNull(resourcePath)) {
            return Optional.empty();
        }
        String[] elements = resourcePath.split("/");
        // acl resource path like '/{project}/acl/{user|group}/{name}.json
        if (elements.length < 4 || !"acl".equals(elements[2]) || StringUtils.isEmpty(elements[1])) {
            return Optional.empty();
        }
        return Optional.of(elements[1]);
    }

    private void clearCache(KylinConfig config, String project) {
        if (!config.isRedisEnabled()) {
            queryCacheManager.onClearProjectCache(project);
        }
    }
}
