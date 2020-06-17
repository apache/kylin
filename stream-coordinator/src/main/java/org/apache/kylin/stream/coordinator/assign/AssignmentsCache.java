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

package org.apache.kylin.stream.coordinator.assign;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.kylin.shaded.com.google.common.cache.Cache;
import org.apache.kylin.shaded.com.google.common.cache.CacheBuilder;
import org.apache.kylin.shaded.com.google.common.cache.RemovalNotification;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.cachesync.Broadcaster;
import org.apache.kylin.metadata.cachesync.Broadcaster.Event;
import org.apache.kylin.metadata.cachesync.Broadcaster.Listener;
import org.apache.kylin.stream.coordinator.StreamMetadataStore;
import org.apache.kylin.stream.coordinator.StreamMetadataStoreFactory;

import org.apache.kylin.shaded.com.google.common.collect.Lists;
import org.apache.kylin.stream.core.model.CubeAssignment;
import org.apache.kylin.stream.core.model.ReplicaSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Introduce AssignmentsCache to reduce pressure of
 * @see StreamMetadataStore .
 */
public class AssignmentsCache {
    private static final Logger logger = LoggerFactory.getLogger(AssignmentsCache.class);

    private static final AssignmentsCache instance = new AssignmentsCache();
    private static final String ASSIGNMENT_ENTITY = "cube_assign";
    private StreamMetadataStore metadataStore;
    private Cache<String, List<ReplicaSet>> cubeAssignmentCache;

    private AssignmentsCache() {
        this.metadataStore = StreamMetadataStoreFactory.getStreamMetaDataStore();
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        cubeAssignmentCache = CacheBuilder.newBuilder()
                .removalListener((RemovalNotification<String, List<ReplicaSet>> notification) -> logger
                        .debug("{} is removed because {} ", notification.getKey(), notification.getCause()))
                .expireAfterWrite(300, TimeUnit.SECONDS)
                .build();
         Broadcaster.getInstance(config).registerListener(new AssignCacheSyncListener(), ASSIGNMENT_ENTITY);
    }

    public static AssignmentsCache getInstance() {
        return instance;
    }

    public List<ReplicaSet> getReplicaSetsByCube(String cubeName) {
        List<ReplicaSet> ret ;
        try {
            ret = cubeAssignmentCache.get(cubeName, () -> {
                List<ReplicaSet> result = Lists.newArrayList();
                CubeAssignment assignment = metadataStore.getAssignmentsByCube(cubeName);
                if(assignment == null){
                    logger.error("Inconsistent metadata for assignment of {}, do check it.", cubeName);
                    return result;
                }
                for (Integer replicaSetID : assignment.getReplicaSetIDs()) {
                    result.add(metadataStore.getReplicaSet(replicaSetID));
                }
                logger.trace("Update assignment with {}", result);
                return result;
            });
        } catch (ExecutionException e){
            logger.warn("Failed to load CubeAssignment", e);
            throw new IllegalStateException("Failed to load CubeAssignment", e);
        }
        return ret;
    }

    public void clearCubeCache(String cubeName) {
        Broadcaster.getInstance(KylinConfig.getInstanceFromEnv()).announce(ASSIGNMENT_ENTITY,
                Broadcaster.Event.UPDATE.getType(), cubeName);
        cubeAssignmentCache.invalidate(cubeName);
    }

    private class AssignCacheSyncListener extends Listener {
        @Override
        public void onEntityChange(Broadcaster broadcaster, String entity, Event event, String cacheKey) {
            cubeAssignmentCache.invalidate(cacheKey);
        }
    }
}
