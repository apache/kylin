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

package org.apache.kylin.engine.streaming;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.MapType;
import com.fasterxml.jackson.databind.type.SimpleType;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
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

    private String formatStreamingOutputPath(String streaming, List<Integer> partitions) {
        return ResourceStore.STREAMING_OUTPUT_RESOURCE_ROOT + "/" + streaming + "_" + StringUtils.join(partitions, "_") + ".json";
    }

    public boolean createOrUpdateKafkaConfig(String name, StreamingConfig config) {
        try {
            getStore().putResource(formatStreamingConfigPath(name), config, StreamingConfig.SERIALIZER);
            return true;
        } catch (IOException e) {
            logger.error("error save resource name:" + name, e);
            throw new RuntimeException("error save resource name:" + name, e);
        }
    }

    public StreamingConfig getStreamingConfig(String name) {
        try {
            return getStore().getResource(formatStreamingConfigPath(name), StreamingConfig.class, StreamingConfig.SERIALIZER);
        } catch (IOException e) {
            logger.error("error get resource name:" + name, e);
            throw new RuntimeException("error get resource name:" + name, e);
        }
    }

    public void saveStreamingConfig(StreamingConfig streamingConfig) throws IOException {
        if (streamingConfig == null || StringUtils.isEmpty(streamingConfig.getName())) {
            throw new IllegalArgumentException();
        }

        String path = formatStreamingConfigPath(streamingConfig.getName());
        getStore().putResource(path, streamingConfig, StreamingConfig.SERIALIZER);
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

    public Map<Integer, Long> getOffset(String streaming, List<Integer> partitions) {
        Collections.sort(partitions);
        final String resPath = formatStreamingOutputPath(streaming, partitions);
        try {
            final InputStream inputStream = getStore().getResource(resPath);
            if (inputStream == null) {
                return Collections.emptyMap();
            }
            final HashMap<Integer, Long> result = mapper.readValue(inputStream, mapType);
            return result;
        } catch (IOException e) {
            logger.error("error get offset, path:" + resPath, e);
            throw new RuntimeException("error get offset, path:" + resPath, e);
        }
    }

    public void updateOffset(String streaming, HashMap<Integer, Long> offset) {
        List<Integer> partitions = Lists.newLinkedList(offset.keySet());
        Collections.sort(partitions);
        final String resPath = formatStreamingOutputPath(streaming, partitions);
        try {
            final ByteArrayOutputStream baos = new ByteArrayOutputStream();
            mapper.writeValue(baos, offset);
            getStore().putResource(resPath, new ByteArrayInputStream(baos.toByteArray()), getStore().getResourceTimestamp(resPath));
        } catch (IOException e) {
            logger.error("error update offset, path:" + resPath, e);
            throw new RuntimeException("error update offset, path:" + resPath, e);
        }
    }

    private final ObjectMapper mapper = new ObjectMapper();
    private final JavaType mapType = MapType.construct(HashMap.class, SimpleType.construct(Integer.class), SimpleType.construct(Long.class));

}
