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

import com.google.common.base.Preconditions;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by qianzhou on 3/25/15.
 */
public class StreamingManager {

    private static final Logger logger = LoggerFactory.getLogger(StreamingManager.class);

    // static cached instances
    private static final ConcurrentHashMap<KylinConfig, StreamingManager> CACHE = new ConcurrentHashMap<KylinConfig, StreamingManager>();

    private KylinConfig config;

    private StreamingManager(KylinConfig config) {
        this.config = config;
    }

    private ResourceStore getStore() {
        return ResourceStore.getStore(this.config);
    }


    public static StreamingManager getInstance(KylinConfig config) {
        StreamingManager r = CACHE.get(config);
        if (r != null) {
            return r;
        }

        synchronized (StreamingManager.class) {
            r = CACHE.get(config);
            if (r != null) {
                return r;
            }
            r = new StreamingManager(config);
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
        return ResourceStore.STREAMING_RESOURCE_ROOT + "/" + name + ".json";
    }

    private String formatStreamingOutputPath(String streaming, int partition) {
        return ResourceStore.STREAMING_OUTPUT_RESOURCE_ROOT + "/" + streaming + "_" + partition + ".json";
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

    public long getOffset(String streaming, int shard) {
        final String resPath = formatStreamingOutputPath(streaming, shard);
        try {
            final InputStream inputStream = getStore().getResource(resPath);
            if (inputStream == null) {
                return 0;
            } else {
                final BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));
                return Long.parseLong(br.readLine());
            }
        } catch (Exception e) {
            logger.error("error get offset, path:" + resPath, e);
            throw new RuntimeException("error get offset, path:" + resPath, e);
        }
    }

    public void updateOffset(String streaming, int shard, long offset) {
        Preconditions.checkArgument(offset >= 0, "offset cannot be smaller than 0");
        final String resPath = formatStreamingOutputPath(streaming, shard);
        try {
            getStore().putResource(resPath, new ByteArrayInputStream(Long.valueOf(offset).toString().getBytes()), getStore().getResourceTimestamp(resPath));
        } catch (IOException e) {
            logger.error("error update offset, path:" + resPath, e);
            throw new RuntimeException("error update offset, path:" + resPath, e);
        }
    }

}
