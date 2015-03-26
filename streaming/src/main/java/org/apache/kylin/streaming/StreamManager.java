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

package org.apache.kylin.streaming;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by qianzhou on 3/25/15.
 */
public class StreamManager {

    private static final Logger logger = LoggerFactory.getLogger(StreamManager.class);

    // static cached instances
    private static final ConcurrentHashMap<KylinConfig, StreamManager> CACHE = new ConcurrentHashMap<KylinConfig, StreamManager>();

    private KylinConfig config;

    private StreamManager(KylinConfig config) {
        this.config = config;
    }

    private ResourceStore getStore() {
        return ResourceStore.getStore(this.config);
    }


    public static StreamManager getInstance(KylinConfig config) {
        StreamManager r = CACHE.get(config);
        if (r != null) {
            return r;
        }

        synchronized (StreamManager.class) {
            r = CACHE.get(config);
            if (r != null) {
                return r;
            }
            r = new StreamManager(config);
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

    private String formatResPath(String name) {
        return ResourceStore.STREAM_RESOURCE_ROOT + "/" + name + ".json";
    }


    public boolean createOrUpdateKafkaConfig(String name, KafkaConfig config) {
        try {
            getStore().putResource(formatResPath(name), config, KafkaConfig.SERIALIZER);
            return true;
        } catch (IOException e) {
            logger.error("error save resource name:" + name, e);
            return false;
        }
    }

    public KafkaConfig getKafkaConfig(String name) {
        try {
            return getStore().getResource(formatResPath(name), KafkaConfig.class, KafkaConfig.SERIALIZER);
        } catch (IOException e) {
            logger.error("error get resource name:" + name, e);
            return null;
        }
    }

}
