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

import java.io.IOException;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.metadata.cachesync.CachedCrudAssist;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 */
public class StreamingManager {

    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(StreamingManager.class);

    public static StreamingManager getInstance(KylinConfig config) {
        return config.getManager(StreamingManager.class);
    }

    // called by reflection
    static StreamingManager newInstance(KylinConfig config) throws IOException {
        return new StreamingManager(config);
    }

    // ============================================================================

    private KylinConfig config;

    // name ==> StreamingConfig
    private CachedCrudAssist<StreamingConfig> crud;

    private StreamingManager(KylinConfig config) throws IOException {
        this.config = config;
        // todo prj
        this.crud = new CachedCrudAssist<StreamingConfig>(getStore(), ResourceStore.STREAMING_RESOURCE_ROOT,
                StreamingConfig.class) {
            @Override
            protected StreamingConfig initEntityAfterReload(StreamingConfig t, String resourceName) {
                return t; // noop
            }
        };

        // touch lower level metadata before registering my listener
        crud.reloadAll();
    }

    private ResourceStore getStore() {
        return ResourceStore.getKylinMetaStore(this.config);
    }

    public StreamingConfig getStreamingConfig(String name) {
        return crud.get(name);
    }

    public List<StreamingConfig> listAllStreaming() {
        return crud.listAll();
    }

    // for test
    List<StreamingConfig> reloadAll() throws IOException {
        crud.reloadAll();
        return listAllStreaming();
    }

    public StreamingConfig createStreamingConfig(StreamingConfig streamingConfig) throws IOException {
        if (streamingConfig == null || StringUtils.isEmpty(streamingConfig.getName())) {
            throw new IllegalArgumentException();
        }
        if (crud.contains(streamingConfig.resourceName()))
            throw new IllegalArgumentException("StreamingConfig '" + streamingConfig.getName() + "' already exists");

        streamingConfig.updateRandomUuid();

        return crud.save(streamingConfig);
    }

    public StreamingConfig updateStreamingConfig(StreamingConfig desc) throws IOException {
        if (desc.getUuid() == null || desc.getName() == null) {
            throw new IllegalArgumentException("SteamingConfig Illegal.");
        }
        if (!crud.contains(desc.resourceName())) {
            throw new IllegalArgumentException("StreamingConfig '" + desc.getName() + "' does not exist.");
        }

        return crud.save(desc);
    }

    public void removeStreamingConfig(StreamingConfig streamingConfig) throws IOException {
        crud.delete(streamingConfig);
    }
}
