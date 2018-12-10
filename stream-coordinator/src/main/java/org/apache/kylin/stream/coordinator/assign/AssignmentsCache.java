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

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ConcurrentMap;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.cachesync.Broadcaster;
import org.apache.kylin.metadata.cachesync.Broadcaster.Event;
import org.apache.kylin.metadata.cachesync.Broadcaster.Listener;
import org.apache.kylin.stream.coordinator.StreamMetadataStore;
import org.apache.kylin.stream.coordinator.StreamMetadataStoreFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.kylin.stream.core.model.CubeAssignment;
import org.apache.kylin.stream.core.model.ReplicaSet;

public class AssignmentsCache {
    private static volatile AssignmentsCache instance = new AssignmentsCache();
    private static final String ASSIGNMENT_ENTITY = "cube_assign";
    private StreamMetadataStore metadataStore;
    private ConcurrentMap<String, List<ReplicaSet>> cubeAssignmentCache;

    private AssignmentsCache() {
        this.metadataStore = StreamMetadataStoreFactory.getStreamMetaDataStore();
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        cubeAssignmentCache = Maps.newConcurrentMap();
        Broadcaster.getInstance(config).registerListener(new AssignCacheSyncListener(), ASSIGNMENT_ENTITY);
    }

    public static AssignmentsCache getInstance() {
        return instance;
    }

    public List<ReplicaSet> getReplicaSetsByCube(String cubeName) {
        if (cubeAssignmentCache.get(cubeName) == null) {
            synchronized (cubeAssignmentCache) {
                if (cubeAssignmentCache.get(cubeName) == null) {
                    List<ReplicaSet> result = Lists.newArrayList();

                    CubeAssignment assignment = metadataStore.getAssignmentsByCube(cubeName);
                    for (Integer replicaSetID : assignment.getReplicaSetIDs()) {
                        result.add(metadataStore.getReplicaSet(replicaSetID));
                    }
                    cubeAssignmentCache.put(cubeName, result);
                }
            }
        }
        return cubeAssignmentCache.get(cubeName);
    }

    public void clearCubeCache(String cubeName) {
        Broadcaster.getInstance(KylinConfig.getInstanceFromEnv()).announce(ASSIGNMENT_ENTITY,
                Broadcaster.Event.UPDATE.getType(), cubeName);
        cubeAssignmentCache.remove(cubeName);
    }

    private class AssignCacheSyncListener extends Listener {
        public void onEntityChange(Broadcaster broadcaster, String entity, Event event, String cacheKey)
                throws IOException {
            cubeAssignmentCache.remove(cacheKey);
        }
    }
}
