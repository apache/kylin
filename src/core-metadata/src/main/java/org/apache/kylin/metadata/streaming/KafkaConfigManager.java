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

package org.apache.kylin.metadata.streaming;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.metadata.cachesync.CachedCrudAssist;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 */
public class KafkaConfigManager {
    private static final Logger logger = LoggerFactory.getLogger(KafkaConfigManager.class);

    public static KafkaConfigManager getInstance(KylinConfig config, String project) {
        return config.getManager(project, KafkaConfigManager.class);
    }

    // called by reflection
    @SuppressWarnings("unused")
    static KafkaConfigManager newInstance(KylinConfig config, String project) {
        return new KafkaConfigManager(config, project);
    }

    // ============================================================================

    private KylinConfig config;

    // name ==> StreamingConfig
    private CachedCrudAssist<KafkaConfig> crud;

    private KafkaConfigManager(KylinConfig config, String project) {
        this.config = config;
        String resourceRootPath = "/" + project + ResourceStore.KAFKA_RESOURCE_ROOT;
        this.crud = new CachedCrudAssist<KafkaConfig>(getStore(), resourceRootPath, KafkaConfig.class) {
            @Override
            protected KafkaConfig initEntityAfterReload(KafkaConfig t, String resourceName) {
                return t; // noop
            }
        };

        // touch lower level metadata before registering my listener
        crud.reloadAll();
    }

    private ResourceStore getStore() {
        return ResourceStore.getKylinMetaStore(this.config);
    }

    public KafkaConfig getKafkaConfig(String id) {
        if (org.apache.commons.lang.StringUtils.isEmpty(id)) {
            return null;
        }
        return crud.get(id);
    }

    public KafkaConfig createKafkaConfig(KafkaConfig kafkaConfig) {
        if (kafkaConfig == null || StringUtils.isEmpty(kafkaConfig.getName())) {
            throw new IllegalArgumentException();
        }
        if (crud.contains(kafkaConfig.resourceName()))
            throw new IllegalArgumentException("Kafka Config '" + kafkaConfig.getName() + "' already exists");

        kafkaConfig.updateRandomUuid();
        return crud.save(kafkaConfig);
    }

    public KafkaConfig updateKafkaConfig(KafkaConfig kafkaConfig) {
        if (!crud.contains(kafkaConfig.resourceName())) {
            throw new IllegalArgumentException("Kafka Config '" + kafkaConfig.getName() + "' does not exist.");
        }
        return crud.save(kafkaConfig);
    }

    public KafkaConfig removeKafkaConfig(String tableIdentity) {
        KafkaConfig kafkaConfig = getKafkaConfig(tableIdentity);
        if (kafkaConfig == null) {
            logger.warn("Dropping Kafka Config '{}' does not exist", tableIdentity);
            return null;
        }
        crud.delete(kafkaConfig);
        logger.info("Dropping Kafka Config '{}'", tableIdentity);
        return kafkaConfig;
    }

    public List<KafkaConfig> listAllKafkaConfigs() {
        return crud.listAll().stream().collect(Collectors.toList());
    }

    public List<KafkaConfig> getKafkaTablesUsingTable(String table) {
        List<KafkaConfig> kafkaConfigs = new ArrayList<>();
        for (KafkaConfig kafkaConfig : listAllKafkaConfigs()) {
            if (kafkaConfig.hasBatchTable() && kafkaConfig.getBatchTable().equals(table))
                kafkaConfigs.add(kafkaConfig);
        }
        return kafkaConfigs;
    }

    public void invalidCache(String resourceName) {
        crud.invalidateCache(resourceName);
    }

}
