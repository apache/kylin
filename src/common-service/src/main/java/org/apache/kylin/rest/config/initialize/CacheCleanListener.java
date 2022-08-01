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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.RawResource;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.transaction.EventListenerRegistry;
import org.apache.kylin.metadata.model.NTableMetadataManager;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.metadata.streaming.KafkaConfigManager;

import com.google.common.collect.Lists;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CacheCleanListener implements EventListenerRegistry.ResourceEventListener {

    private static final List<Pattern> PROJECT_RESOURCE_PATTERN = Lists
            .newArrayList(Pattern.compile(ResourceStore.PROJECT_ROOT + "/([^/]+)$"));

    private static final List<Pattern> TABLE_RESOURCE_PATTERN = Lists.newArrayList(
            Pattern.compile("/([^/]+)" + ResourceStore.TABLE_RESOURCE_ROOT + "/([^/]+)"),
            Pattern.compile("/([^/]+)" + ResourceStore.TABLE_EXD_RESOURCE_ROOT + "/([^/]+)"),
            Pattern.compile("/([^/]+)" + ResourceStore.EXTERNAL_FILTER_RESOURCE_ROOT + "/([^/]+)"));

    private static final List<Pattern> KAFKA_RESOURCE_PATTERN = Lists
            .newArrayList(Pattern.compile("/([^/]+)" + ResourceStore.KAFKA_RESOURCE_ROOT + "/([^/]+)"));

    @Override
    public void onUpdate(KylinConfig config, RawResource rawResource) {
        // Do nothing. Cache will update internally because mvcc is changed.
    }

    @Override
    public void onDelete(KylinConfig config, String resPath) {
        try {
            PROJECT_RESOURCE_PATTERN.forEach(pattern -> {
                String project = extractProject(resPath, pattern);
                if (StringUtils.isNotBlank(project)) {
                    NProjectManager.getInstance(config).invalidCache(project);
                }
            });
            TABLE_RESOURCE_PATTERN.forEach(pattern -> {
                String project = extractProject(resPath, pattern);
                String table = extractTable(resPath, pattern);
                if (StringUtils.isNotBlank(project) && StringUtils.isNotBlank(table)) {
                    NTableMetadataManager.getInstance(config, project).invalidCache(table);
                }
            });
            KAFKA_RESOURCE_PATTERN.forEach(pattern -> {
                String project = extractProject(resPath, pattern);
                String kafkaTableName = extractTable(resPath, pattern);
                if (StringUtils.isNotBlank(project) && StringUtils.isNotBlank(kafkaTableName)) {
                    KafkaConfigManager.getInstance(config, project).invalidCache(kafkaTableName);
                }
            });
        } catch (Exception e) {
            log.error("Unexpected error happened! Clean resource {} cache failed.", resPath, e);
        }

    }

    private String extractProject(String resPath, Pattern pattern) {
        Matcher matcher = pattern.matcher(resPath);
        if (matcher.find()) {
            return matcher.group(1).replace(".json", "");
        }
        return null;
    }

    private String extractTable(String resPath, Pattern pattern) {
        Matcher matcher = pattern.matcher(resPath);
        if (matcher.find() && matcher.groupCount() == 2) {
            return matcher.group(2).replace(".json", "");
        }
        return null;
    }
}
