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

import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.cube.model.AggregationGroup;
import org.apache.kylin.cube.model.CubeDesc;

import org.apache.kylin.shaded.com.google.common.base.Preconditions;
import org.apache.kylin.shaded.com.google.common.collect.Lists;

/**
 * Defines a cuboid tree, rooted by the base cuboid. A parent cuboid generates its child cuboids.
 */
abstract public class CuboidScheduler {
    
    public static CuboidScheduler getInstance(CubeDesc cubeDesc) {
        String clzName = cubeDesc.getConfig().getCuboidScheduler();
        try {
            Class<? extends CuboidScheduler> clz = ClassUtil.forName(clzName, CuboidScheduler.class);
            return clz.getConstructor(CubeDesc.class).newInstance(cubeDesc);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    
    // ============================================================================
    
    final protected CubeDesc cubeDesc;
    
    protected CuboidScheduler(CubeDesc cubeDesc) {
        this.cubeDesc = cubeDesc;
    }

    /** Returns all cuboids on the tree. */
    abstract public Set<Long> getAllCuboidIds();
    
    /** Returns the number of all cuboids. */
    abstract public int getCuboidCount();
    
    /** Returns the child cuboids of a parent. */
    abstract public List<Long> getSpanningCuboid(long parentCuboid);
    
    /** Returns a valid cuboid that best matches the request cuboid. */
    abstract public long findBestMatchCuboid(long requestCuboid);

    /** optional */
    abstract public Set<Long> calculateCuboidsForAggGroup(AggregationGroup agg);

    // ============================================================================
    
    private transient List<List<Long>> cuboidsByLayer;

    public long getBaseCuboidId() {
        return Cuboid.getBaseCuboidId(cubeDesc);
    }

    public CubeDesc getCubeDesc() {
        return cubeDesc;
    }

    /** Checks whether a cuboid is valid or not. */
    public boolean isValid(long requestCuboid) {
        return getAllCuboidIds().contains(requestCuboid);
    }

    /**
     * Get cuboids by layer. It's built from pre-expanding tree.
     * @return layered cuboids
     */
    public List<List<Long>> getCuboidsByLayer() {
        if (cuboidsByLayer != null) {
            return cuboidsByLayer;
        }

        int totalNum = 0;
        cuboidsByLayer = Lists.newArrayList();

        cuboidsByLayer.add(Collections.singletonList(Cuboid.getBaseCuboidId(cubeDesc)));
        totalNum++;

        List<Long> lastLayer = cuboidsByLayer.get(cuboidsByLayer.size() - 1);
        while (!lastLayer.isEmpty()) {
            List<Long> newLayer = Lists.newArrayList();
            for (Long parent : lastLayer) {
                newLayer.addAll(getSpanningCuboid(parent));
            }
            if (newLayer.isEmpty()) {
                break;
            }
            cuboidsByLayer.add(newLayer);
            totalNum += newLayer.size();
            lastLayer = newLayer;
        }

        int size = getAllCuboidIds().size();
        Preconditions.checkState(totalNum == size, "total Num: " + totalNum + " actual size: " + size);
        return cuboidsByLayer;
    }

    /**
     * Get cuboid level count except base cuboid
     * @return
     */
    public int getBuildLevel() {
        return getCuboidsByLayer().size() - 1;
    }
    
    /** Returns the key for what this cuboid scheduler responsible for. */
    public String getCuboidCacheKey() {
        return cubeDesc.getName();
    }
    
}
