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

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.MapType;
import com.fasterxml.jackson.databind.type.SimpleType;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.source.kafka.config.KafkaConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

/**
 */
public class KafkaConfigManager {

    private static final Logger logger = LoggerFactory.getLogger(KafkaConfigManager.class);

    // static cached instances
    private static final ConcurrentHashMap<KylinConfig, KafkaConfigManager> CACHE = new ConcurrentHashMap<KylinConfig, KafkaConfigManager>();

    private KylinConfig config;

    private KafkaConfigManager(KylinConfig config) {
        this.config = config;
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
            r = new KafkaConfigManager(config);
            CACHE.put(config, r);
            if (CACHE.size() > 1) {
                logger.warn("More than one cubemanager singleton exist");
            }
            return r;
        }
    }

    private boolean checkExistence(String name) {
        return true;
    }

    private String formatStreamingConfigPath(String name) {
        return ResourceStore.KAfKA_RESOURCE_ROOT + "/" + name + ".json";
    }

    public boolean createOrUpdateKafkaConfig(String name, KafkaConfig config) {
        try {
            getStore().putResource(formatStreamingConfigPath(name), config, KafkaConfig.SERIALIZER);
            return true;
        } catch (IOException e) {
            logger.error("error save resource name:" + name, e);
            throw new RuntimeException("error save resource name:" + name, e);
        }
    }

    public KafkaConfig getKafkaConfig(String name) {
        try {
            return getStore().getResource(formatStreamingConfigPath(name), KafkaConfig.class, KafkaConfig.SERIALIZER);
        } catch (IOException e) {
            logger.error("error get resource name:" + name, e);
            throw new RuntimeException("error get resource name:" + name, e);
        }
    }

    public void saveKafkaConfig(KafkaConfig kafkaConfig) throws IOException {
        if (kafkaConfig == null || StringUtils.isEmpty(kafkaConfig.getName())) {
            throw new IllegalArgumentException();
        }

        String path = formatStreamingConfigPath(kafkaConfig.getName());
        getStore().putResource(path, kafkaConfig, KafkaConfig.SERIALIZER);
    }

    private final ObjectMapper mapper = new ObjectMapper();
    private final JavaType mapType = MapType.construct(HashMap.class, SimpleType.construct(Integer.class), SimpleType.construct(Long.class));

}
