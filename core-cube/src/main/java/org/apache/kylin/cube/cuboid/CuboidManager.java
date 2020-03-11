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
import java.util.Map;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.CubeInstance;

import org.apache.kylin.shaded.com.google.common.collect.Maps;
import org.apache.kylin.cube.model.CubeDesc;

/**
 * A cuboid cache.
 */
public class CuboidManager {

    public static CuboidManager getInstance(KylinConfig config) {
        return config.getManager(CuboidManager.class);
    }

    // called by reflection
    static CuboidManager newInstance(KylinConfig config) throws IOException {
        return new CuboidManager(config);
    }
    
    // ============================================================================

    @SuppressWarnings("unused")
    final private KylinConfig config;
    final private Map<String, Map<Long, Cuboid>> schedulerCuboidCache = Maps.newConcurrentMap();
    
    private CuboidManager(KylinConfig config) {
        this.config = config;
    }
    
    public Cuboid findById(CuboidScheduler cuboidScheduler, long cuboidID) {
        Map<Long, Cuboid> cubeCache = schedulerCuboidCache.get(cuboidScheduler.getCuboidCacheKey());
        if (cubeCache == null) {
            cubeCache = Maps.newConcurrentMap();
            schedulerCuboidCache.put(cuboidScheduler.getCuboidCacheKey(), cubeCache);
        }
        Cuboid cuboid = cubeCache.get(cuboidID);
        if (cuboid == null) {
            long validCuboidID = cuboidScheduler.findBestMatchCuboid(cuboidID);
            cuboid = new Cuboid(cuboidScheduler.getCubeDesc(), cuboidID, validCuboidID);
            cubeCache.put(cuboidID, cuboid);
        }
        return cuboid;
    }

    public Cuboid findMandatoryId(CubeDesc cubeDesc, long cuboidID) {
        Map<Long, Cuboid> cubeCache = schedulerCuboidCache.get(cubeDesc.getName());
        if (cubeCache == null) {
            cubeCache = Maps.newConcurrentMap();
            schedulerCuboidCache.put(cubeDesc.getName(), cubeCache);
        }
        Cuboid cuboid = cubeCache.get(cuboidID);
        if (cuboid == null) {
            cuboid = new Cuboid(cubeDesc, cuboidID, cuboidID);
            cubeCache.put(cuboidID, cuboid);
        }
        return cuboid;
    }

    public void clearCache(String cacheKey) {
        schedulerCuboidCache.remove(cacheKey);
    }
    
    public void clearCache(CubeInstance cubeInstance) {
        schedulerCuboidCache.remove(cubeInstance.getCuboidScheduler().getCuboidCacheKey());
    }

}
