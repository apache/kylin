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

package org.apache.kylin.cube.cuboid;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.metadata.cachesync.Broadcaster;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class TreeCuboidSchedulerManager {
    private static ConcurrentMap<String, TreeCuboidScheduler> cache = Maps.newConcurrentMap();

    private class TreeCuboidSchedulerSyncListener extends Broadcaster.Listener {
        @Override
        public void onClearAll(Broadcaster broadcaster) throws IOException {
            cache.clear();
        }

        @Override
        public void onEntityChange(Broadcaster broadcaster, String entity, Broadcaster.Event event, String cacheKey)
                throws IOException {
            cache.remove(cacheKey);
        }
    }

    public TreeCuboidSchedulerManager() {
        Broadcaster.getInstance(KylinConfig.getInstanceFromEnv())
                .registerListener(new TreeCuboidSchedulerSyncListener(), "cube");
    }

    private static TreeCuboidSchedulerManager instance = new TreeCuboidSchedulerManager();

    public static TreeCuboidSchedulerManager getInstance() {
        return instance;
    }

    /**
     *
     * @param cubeName
     * @return null if the cube has no pre-built cuboids
     */
    public TreeCuboidScheduler getTreeCuboidScheduler(String cubeName) {
        TreeCuboidScheduler result = cache.get(cubeName);
        if (result == null) {
            CubeManager cubeManager = CubeManager.getInstance(KylinConfig.getInstanceFromEnv());
            CubeInstance cubeInstance = cubeManager.getCube(cubeName);
            if (cubeInstance == null) {
                return null;
            }
            TreeCuboidScheduler treeCuboidScheduler = getTreeCuboidScheduler(cubeInstance.getDescriptor(),
                    cubeManager.getCube(cubeName).getCuboids());
            if (treeCuboidScheduler == null) {
                return null;
            }
            cache.put(cubeName, treeCuboidScheduler);
            result = treeCuboidScheduler;
        }
        return result;
    }

    public TreeCuboidScheduler getTreeCuboidScheduler(CubeDesc cubeDesc, Map<Long, Long> cuboidsWithRowCnt) {
        if (cuboidsWithRowCnt == null || cuboidsWithRowCnt.isEmpty()) {
            return null;
        }
        return getTreeCuboidScheduler(cubeDesc, Lists.newArrayList(cuboidsWithRowCnt.keySet()), cuboidsWithRowCnt);
    }

    public TreeCuboidScheduler getTreeCuboidScheduler(CubeDesc cubeDesc, List<Long> cuboidIds,
            Map<Long, Long> cuboidsWithRowCnt) {
        if (cuboidIds == null || cuboidsWithRowCnt == null) {
            return null;
        }
        TreeCuboidScheduler treeCuboidScheduler = new TreeCuboidScheduler(cubeDesc, cuboidIds,
                new TreeCuboidScheduler.CuboidCostComparator(cuboidsWithRowCnt));
        return treeCuboidScheduler;
    }
}
