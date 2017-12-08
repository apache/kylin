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

package org.apache.kylin.source.kafka;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.AutoReadWriteLock;
import org.apache.kylin.common.util.AutoReadWriteLock.AutoLock;
import org.apache.kylin.metadata.cachesync.Broadcaster;
import org.apache.kylin.metadata.cachesync.Broadcaster.Event;
import org.apache.kylin.metadata.cachesync.CachedCrudAssist;
import org.apache.kylin.metadata.cachesync.CaseInsensitiveStringCache;
import org.apache.kylin.source.kafka.config.KafkaConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 */
public class KafkaConfigManager {

    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(KafkaConfigManager.class);

    public static KafkaConfigManager getInstance(KylinConfig config) {
        return config.getManager(KafkaConfigManager.class);
    }

    // called by reflection
    static KafkaConfigManager newInstance(KylinConfig config) throws IOException {
        return new KafkaConfigManager(config);
    }

    // ============================================================================

    private KylinConfig config;

    // name ==> StreamingConfig
    private CaseInsensitiveStringCache<KafkaConfig> kafkaMap;
    private CachedCrudAssist<KafkaConfig> crud;
    private AutoReadWriteLock lock = new AutoReadWriteLock();

    private KafkaConfigManager(KylinConfig config) throws IOException {
        this.config = config;
        this.kafkaMap = new CaseInsensitiveStringCache<KafkaConfig>(config, "kafka");
        this.crud = new CachedCrudAssist<KafkaConfig>(getStore(), ResourceStore.KAFKA_RESOURCE_ROOT, KafkaConfig.class,
                kafkaMap) {
            @Override
            protected KafkaConfig initEntityAfterReload(KafkaConfig t, String resourceName) {
                return t; // noop
            }
        };

        // touch lower level metadata before registering my listener
        crud.reloadAll();
        Broadcaster.getInstance(config).registerListener(new KafkaSyncListener(), "kafka");
    }

    private class KafkaSyncListener extends Broadcaster.Listener {

        @Override
        public void onEntityChange(Broadcaster broadcaster, String entity, Event event, String cacheKey)
                throws IOException {
            try (AutoLock l = lock.lockForWrite()) {
                if (event == Event.DROP)
                    kafkaMap.removeLocal(cacheKey);
                else
                    crud.reloadQuietly(cacheKey);
            }
        }
    }

    private ResourceStore getStore() {
        return ResourceStore.getStore(this.config);
    }

    public KafkaConfig getKafkaConfig(String name) {
        try (AutoLock l = lock.lockForRead()) {
            return kafkaMap.get(name);
        }
    }

    public List<KafkaConfig> listAllKafkaConfigs() {
        try (AutoLock l = lock.lockForRead()) {
            return new ArrayList(kafkaMap.values());
        }
    }

    public boolean createKafkaConfig(KafkaConfig kafkaConfig) throws IOException {
        try (AutoLock l = lock.lockForWrite()) {

            if (kafkaMap.containsKey(kafkaConfig.resourceName()))
                throw new IllegalArgumentException("KafkaConfig '" + kafkaConfig.getName() + "' already exists");

            kafkaConfig.updateRandomUuid();
            checkKafkaConfig(kafkaConfig);

            crud.save(kafkaConfig);
            return true;
        }
    }

    public KafkaConfig updateKafkaConfig(KafkaConfig kafkaConfig) throws IOException {
        try (AutoLock l = lock.lockForWrite()) {

            if (!kafkaMap.containsKey(kafkaConfig.resourceName()))
                throw new IllegalArgumentException("KafkaConfig '" + kafkaConfig.getName() + "' does not exist.");

            checkKafkaConfig(kafkaConfig);
            
            return crud.save(kafkaConfig);
        }
    }

    private void checkKafkaConfig(KafkaConfig kafkaConfig) {
        if (kafkaConfig == null || StringUtils.isEmpty(kafkaConfig.getName())) {
            throw new IllegalArgumentException();
        }

        if (StringUtils.isEmpty(kafkaConfig.getTopic())) {
            throw new IllegalArgumentException("No topic info");
        }

        if (kafkaConfig.getKafkaClusterConfigs() == null || kafkaConfig.getKafkaClusterConfigs().size() == 0) {
            throw new IllegalArgumentException("No cluster info");
        }
    }

    // remove kafkaConfig
    public void removeKafkaConfig(KafkaConfig kafkaConfig) throws IOException {
        try (AutoLock l = lock.lockForWrite()) {
            crud.delete(kafkaConfig);
        }
    }

}
