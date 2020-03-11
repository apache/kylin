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

package org.apache.kylin.stream.core.source;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.JsonSerializer;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.Serializer;
import org.apache.kylin.metadata.MetadataConstants;
import org.apache.kylin.stream.core.exception.StreamingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kylin.shaded.com.google.common.collect.Lists;

/**
 */
public class StreamingSourceConfigManager {

    public static final Serializer<StreamingSourceConfig> STREAMING_SERIALIZER = new JsonSerializer<StreamingSourceConfig>(
            StreamingSourceConfig.class);
    private static final Logger logger = LoggerFactory.getLogger(StreamingSourceConfigManager.class);
    // static cached instances
    private static final ConcurrentHashMap<KylinConfig, StreamingSourceConfigManager> CACHE = new ConcurrentHashMap<KylinConfig, StreamingSourceConfigManager>();
    private KylinConfig config;

    private StreamingSourceConfigManager(KylinConfig config) throws IOException {
        this.config = config;
    }

    public static StreamingSourceConfigManager getInstance(KylinConfig config) {
        StreamingSourceConfigManager r = CACHE.get(config);
        if (r != null) {
            return r;
        }

        synchronized (StreamingSourceConfigManager.class) {
            r = CACHE.get(config);
            if (r != null) {
                return r;
            }
            try {
                r = new StreamingSourceConfigManager(config);
                CACHE.put(config, r);
                if (CACHE.size() > 1) {
                    logger.warn("More than one streamingManager singleton exist");
                }
                return r;
            } catch (IOException e) {
                throw new IllegalStateException("Failed to init StreamingManager from " + config, e);
            }
        }
    }

    private ResourceStore getStore() {
        return ResourceStore.getStore(this.config);
    }

    public List<StreamingSourceConfig> listAllStreaming() throws IOException {
        List<StreamingSourceConfig> results = Lists.newArrayList();
        ResourceStore store = getStore();
        logger.info("Load all streaming metadata from folder "
                + store.getReadableResourcePath(ResourceStore.STREAMING_V2_RESOURCE_ROOT));

        List<String> paths = store.collectResourceRecursively(ResourceStore.STREAMING_V2_RESOURCE_ROOT,
                MetadataConstants.FILE_SURFIX);
        for (String path : paths) {
            StreamingSourceConfig streamingSourceConfig;
            try {
                streamingSourceConfig = loadStreamingConfigAt(path);
            } catch (Exception e) {
                logger.error("Error loading streaming desc " + path, e);
                continue;
            }
            if (path.equals(streamingSourceConfig.getResourcePath()) == false) {
                logger.error("Skip suspicious desc at " + path + ", " + streamingSourceConfig + " should be at "
                        + streamingSourceConfig.getResourcePath());
                continue;
            }
            results.add(streamingSourceConfig);
        }

        logger.debug("Loaded " + results.size() + " StreamingSourceConfig(s)");
        return results;
    }

    /**
     * Reload StreamingSourceConfig from resource store It will be triggered by an desc
     * update event.
     *
     * @param name
     * @throws IOException
     */
    public StreamingSourceConfig reloadStreamingConfigLocal(String name) throws IOException {

        // Save Source
        String path = StreamingSourceConfig.concatResourcePath(name);

        // Reload the StreamingSourceConfig
        StreamingSourceConfig ndesc = loadStreamingConfigAt(path);
        return ndesc;
    }

    // remove streamingSourceConfig
    public void removeStreamingConfig(StreamingSourceConfig streamingSourceConfig) throws IOException {
        String path = streamingSourceConfig.getResourcePath();
        getStore().deleteResource(path);
    }

    public StreamingSourceConfig getConfig(String name) {
        name = name.toUpperCase(Locale.ROOT);
        try {
            return reloadStreamingConfigLocal(name);
        } catch (IOException e) {
            throw new StreamingException(e);
        }
    }

    /**
     *
     * @param desc
     * @return
     * @throws IOException
     */
    public StreamingSourceConfig updateStreamingConfig(StreamingSourceConfig desc) throws IOException {
        // Validate CubeDesc
        if (desc.getUuid() == null || desc.getName() == null) {
            throw new IllegalArgumentException("SteamingConfig Illegal.");
        }

        // Save Source
        String path = desc.getResourcePath();
        getStore().putResource(path, desc, System.currentTimeMillis(), STREAMING_SERIALIZER);

        // Reload the StreamingSourceConfig
        StreamingSourceConfig ndesc = loadStreamingConfigAt(path);

        return ndesc;
    }

    public StreamingSourceConfig saveStreamingConfig(StreamingSourceConfig streamingSourceConfig) throws IOException {
        if (streamingSourceConfig == null || StringUtils.isEmpty(streamingSourceConfig.getName())) {
            throw new IllegalArgumentException();
        }

        String path = StreamingSourceConfig.concatResourcePath(streamingSourceConfig.getName());
        getStore().putResource(path, streamingSourceConfig, System.currentTimeMillis(),
                StreamingSourceConfig.SERIALIZER);
        return streamingSourceConfig;
    }

    private StreamingSourceConfig loadStreamingConfigAt(String path) throws IOException {
        ResourceStore store = getStore();
        StreamingSourceConfig streamingDesc = store.getResource(path, STREAMING_SERIALIZER);
        if (streamingDesc == null) {
            return null;
        }
        if (StringUtils.isBlank(streamingDesc.getName())) {
            throw new IllegalStateException("StreamingSourceConfig name must not be blank");
        }
        return streamingDesc;
    }
}
