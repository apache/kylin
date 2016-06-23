/*
 *
 *
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *
 *  contributor license agreements. See the NOTICE file distributed with
 *
 *  this work for additional information regarding copyright ownership.
 *
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *
 *  (the "License"); you may not use this file except in compliance with
 *
 *  the License. You may obtain a copy of the License at
 *
 *
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *
 *
 *  Unless required by applicable law or agreed to in writing, software
 *
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *
 *  See the License for the specific language governing permissions and
 *
 *  limitations under the License.
 *
 * /
 */

package org.apache.kylin.source.kafka;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.JsonSerializer;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.Serializer;
import org.apache.kylin.common.restclient.Broadcaster;
import org.apache.kylin.common.restclient.CaseInsensitiveStringCache;
import org.apache.kylin.metadata.MetadataConstants;
import org.apache.kylin.source.kafka.config.KafkaConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 */
public class KafkaConfigManager {

    private static final Logger logger = LoggerFactory.getLogger(KafkaConfigManager.class);

    // static cached instances
    private static final ConcurrentHashMap<KylinConfig, KafkaConfigManager> CACHE = new ConcurrentHashMap<KylinConfig, KafkaConfigManager>();

    private KylinConfig config;

    // name ==> StreamingConfig
    private CaseInsensitiveStringCache<KafkaConfig> kafkaMap;

    public static final Serializer<KafkaConfig> KAFKA_SERIALIZER = new JsonSerializer<KafkaConfig>(KafkaConfig.class);

    public static void clearCache() {
        CACHE.clear();
    }

    private KafkaConfigManager(KylinConfig config) throws IOException {
        this.config = config;
        this.kafkaMap = new CaseInsensitiveStringCache<KafkaConfig>(config, Broadcaster.TYPE.KAFKA);
        reloadAllKafkaConfig();
    }

    private ResourceStore getStore() {
        return ResourceStore.getStore(this.config);
    }

    public static KafkaConfigManager getInstance(KylinConfig config) {
        KafkaConfigManager r = CACHE.get(config);
        if (r != null) {
            return r;
        }

        synchronized (KafkaConfigManager.class) {
            r = CACHE.get(config);
            if (r != null) {
                return r;
            }
            try {
                r = new KafkaConfigManager(config);
                CACHE.put(config, r);
                if (CACHE.size() > 1) {
                    logger.warn("More than one singleton exist");
                }
                return r;
            } catch (IOException e) {
                throw new IllegalStateException("Failed to init KafkaConfigManager from " + config, e);
            }
        }
    }

    public List<KafkaConfig> listAllKafkaConfigs() {
        return new ArrayList(kafkaMap.values());
    }

    /**
     * Reload KafkaConfig from resource store It will be triggered by an desc
     * update event.
     *
     * @param name
     * @throws IOException
     */
    public KafkaConfig reloadKafkaConfigLocal(String name) throws IOException {

        // Save Source
        String path = KafkaConfig.concatResourcePath(name);

        // Reload the KafkaConfig
        KafkaConfig ndesc = loadKafkaConfigAt(path);

        // Here replace the old one
        kafkaMap.putLocal(ndesc.getName(), ndesc);
        return ndesc;
    }

    public boolean createKafkaConfig(String name, KafkaConfig config) {

        if (config == null || StringUtils.isEmpty(config.getName())) {
            throw new IllegalArgumentException();
        }

        if (kafkaMap.containsKey(config.getName()))
            throw new IllegalArgumentException("KafkaConfig '" + config.getName() + "' already exists");
        try {
            getStore().putResource(KafkaConfig.concatResourcePath(name), config, KafkaConfig.SERIALIZER);
            kafkaMap.put(config.getName(), config);
            return true;
        } catch (IOException e) {
            logger.error("error save resource name:" + name, e);
            throw new RuntimeException("error save resource name:" + name, e);
        }
    }

    public KafkaConfig updateKafkaConfig(KafkaConfig desc) throws IOException {
        // Validate KafkaConfig
        if (desc.getUuid() == null || desc.getName() == null) {
            throw new IllegalArgumentException();
        }
        String name = desc.getName();
        if (!kafkaMap.containsKey(name)) {
            throw new IllegalArgumentException("KafkaConfig '" + name + "' does not exist.");
        }

        // Save Source
        String path = desc.getResourcePath();
        getStore().putResource(path, desc, KAFKA_SERIALIZER);

        // Reload the KafkaConfig
        KafkaConfig ndesc = loadKafkaConfigAt(path);
        // Here replace the old one
        kafkaMap.put(ndesc.getName(), desc);

        return ndesc;
    }

    private KafkaConfig loadKafkaConfigAt(String path) throws IOException {
        ResourceStore store = getStore();
        KafkaConfig kafkaConfig = store.getResource(path, KafkaConfig.class, KAFKA_SERIALIZER);

        if (StringUtils.isBlank(kafkaConfig.getName())) {
            throw new IllegalStateException("KafkaConfig name must not be blank");
        }
        return kafkaConfig;
    }

    public KafkaConfig getKafkaConfig(String name) {
        return kafkaMap.get(name);
    }

    public void saveKafkaConfig(KafkaConfig kafkaConfig) throws IOException {
        if (kafkaConfig == null || StringUtils.isEmpty(kafkaConfig.getName())) {
            throw new IllegalArgumentException();
        }

        String path = KafkaConfig.concatResourcePath(kafkaConfig.getName());
        getStore().putResource(path, kafkaConfig, KafkaConfig.SERIALIZER);
    }

    // remove kafkaConfig
    public void removeKafkaConfig(KafkaConfig kafkaConfig) throws IOException {
        String path = kafkaConfig.getResourcePath();
        getStore().deleteResource(path);
        kafkaMap.remove(kafkaConfig.getName());
    }

    private void reloadAllKafkaConfig() throws IOException {
        ResourceStore store = getStore();
        logger.info("Reloading Kafka Metadata from folder " + store.getReadableResourcePath(ResourceStore.KAFKA_RESOURCE_ROOT));

        kafkaMap.clear();

        List<String> paths = store.collectResourceRecursively(ResourceStore.KAFKA_RESOURCE_ROOT, MetadataConstants.FILE_SURFIX);
        for (String path : paths) {
            KafkaConfig kafkaConfig;
            try {
                kafkaConfig = loadKafkaConfigAt(path);
            } catch (Exception e) {
                logger.error("Error loading kafkaConfig desc " + path, e);
                continue;
            }
            if (path.equals(kafkaConfig.getResourcePath()) == false) {
                logger.error("Skip suspicious desc at " + path + ", " + kafkaConfig + " should be at " + kafkaConfig.getResourcePath());
                continue;
            }
            if (kafkaMap.containsKey(kafkaConfig.getName())) {
                logger.error("Dup KafkaConfig name '" + kafkaConfig.getName() + "' on path " + path);
                continue;
            }

            kafkaMap.putLocal(kafkaConfig.getName(), kafkaConfig);
        }

        logger.debug("Loaded " + kafkaMap.size() + " KafkaConfig(s)");
    }

}
