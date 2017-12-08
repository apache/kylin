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
    private CaseInsensitiveStringCache<StreamingConfig> streamingMap;
    private CachedCrudAssist<StreamingConfig> crud;
    private AutoReadWriteLock lock = new AutoReadWriteLock();

    private StreamingManager(KylinConfig config) throws IOException {
        this.config = config;
        this.streamingMap = new CaseInsensitiveStringCache<StreamingConfig>(config, "streaming");
        this.crud = new CachedCrudAssist<StreamingConfig>(getStore(), ResourceStore.STREAMING_RESOURCE_ROOT,
                StreamingConfig.class, streamingMap) {
            @Override
            protected StreamingConfig initEntityAfterReload(StreamingConfig t, String resourceName) {
                return t; // noop
            }
        };

        // touch lower level metadata before registering my listener
        crud.reloadAll();
        Broadcaster.getInstance(config).registerListener(new StreamingSyncListener(), "streaming");
    }

    private class StreamingSyncListener extends Broadcaster.Listener {

        @Override
        public void onEntityChange(Broadcaster broadcaster, String entity, Event event, String cacheKey)
                throws IOException {
            String streamingName = cacheKey;

            try (AutoLock l = lock.lockForWrite()) {
                if (event == Event.DROP)
                    streamingMap.removeLocal(streamingName);
                else
                    crud.reloadQuietly(streamingName);
            }
        }
    }

    private ResourceStore getStore() {
        return ResourceStore.getStore(this.config);
    }

    public StreamingConfig getStreamingConfig(String name) {
        try (AutoLock l = lock.lockForRead()) {
            return streamingMap.get(name);
        }
    }

    public List<StreamingConfig> listAllStreaming() {
        try (AutoLock l = lock.lockForRead()) {
            return new ArrayList<>(streamingMap.values());
        }
    }
    
    // for test
    List<StreamingConfig> reloadAll() throws IOException {
        try (AutoLock l = lock.lockForWrite()) {
            crud.reloadAll();
            return listAllStreaming();
        }        
    }

    public StreamingConfig createStreamingConfig(StreamingConfig streamingConfig) throws IOException {
        try (AutoLock l = lock.lockForWrite()) {
            if (streamingConfig == null || StringUtils.isEmpty(streamingConfig.getName())) {
                throw new IllegalArgumentException();
            }
            if (streamingMap.containsKey(streamingConfig.resourceName()))
                throw new IllegalArgumentException(
                        "StreamingConfig '" + streamingConfig.getName() + "' already exists");

            streamingConfig.updateRandomUuid();
            
            return crud.save(streamingConfig);
        }
    }

    public StreamingConfig updateStreamingConfig(StreamingConfig desc) throws IOException {
        try (AutoLock l = lock.lockForWrite()) {
            if (desc.getUuid() == null || desc.getName() == null) {
                throw new IllegalArgumentException("SteamingConfig Illegal.");
            }
            if (!streamingMap.containsKey(desc.resourceName())) {
                throw new IllegalArgumentException("StreamingConfig '" + desc.getName() + "' does not exist.");
            }

            return crud.save(desc);
        }
    }

    public void removeStreamingConfig(StreamingConfig streamingConfig) throws IOException {
        try (AutoLock l = lock.lockForWrite()) {
            crud.delete(streamingConfig);
        }
    }

}
