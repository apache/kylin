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

/**
 */

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.kylin.cube.model.AggregationGroup;
import org.apache.kylin.cube.model.CubeDesc;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class CuboidScheduler implements java.io.Serializable {

    private final CubeDesc cubeDesc;
    private final long max;
    private final Map<Long, List<Long>> cache;
    private List<List<Long>> cuboidsByLayer;

    public CuboidScheduler(CubeDesc cubeDesc) {
        this.cubeDesc = cubeDesc;
        int size = this.cubeDesc.getRowkey().getRowKeyColumns().length;
        this.max = (long) Math.pow(2, size) - 1;
        this.cache = new ConcurrentHashMap<Long, List<Long>>();
    }

    public long getParent(long child) {
        List<Long> candidates = Lists.newArrayList();
        long baseCuboidID = Cuboid.getBaseCuboidId(cubeDesc);
        if (child == baseCuboidID || !Cuboid.isValid(cubeDesc, child)) {
            throw new IllegalStateException();
        }
        for (AggregationGroup agg : Cuboid.getValidAggGroupForCuboid(cubeDesc, child)) {
            boolean thisAggContributed = false;
            if (agg.getPartialCubeFullMask() == child) {
                //                candidates.add(baseCuboidID);
                //                continue;
                return baseCuboidID;

            }

            //+1 dim

            //add one normal dim (only try the lowest dim)
            long normalDimsMask = (agg.getNormalDimsMask() & ~child);
            if (normalDimsMask != 0) {
                candidates.add(child | Long.lowestOneBit(normalDimsMask));
                thisAggContributed = true;
            }

            for (AggregationGroup.HierarchyMask hierarchyMask : agg.getHierarchyMasks()) {
                if ((child & hierarchyMask.fullMask) == 0) {
                    candidates.add(child | hierarchyMask.dims[0]);
                    thisAggContributed = true;
                } else {
                    for (int i = hierarchyMask.allMasks.length - 1; i >= 0; i--) {
                        if ((child & hierarchyMask.allMasks[i]) == hierarchyMask.allMasks[i]) {
                            if (i == hierarchyMask.allMasks.length - 1) {
                                continue;//match the full hierarchy
                            }
                            if ((agg.getJointDimsMask() & hierarchyMask.dims[i + 1]) == 0) {
                                if ((child & hierarchyMask.dims[i + 1]) == 0) {
                                    //only when the hierarchy dim is not among joints
                                    candidates.add(child | hierarchyMask.dims[i + 1]);
                                    thisAggContributed = true;
                                }
                            }
                            break;//if hierarchyMask 111 is matched, won't check 110 or 100
                        }
                    }
                }
            }

            if (thisAggContributed) {
                //next section is going to append more than 2 dim to child
                //thisAggContributed means there's already 1 dim added to child
                //which can safely prune the 2+ dim candidates.
                continue;
            }

            //2+ dim candidates
            for (long joint : agg.getJoints()) {
                if ((child & joint) == 0) {
                    candidates.add(child | joint);
                }
            }
        }

        if (candidates.size() == 0) {
            throw new IllegalStateException();
        }

        return Collections.min(candidates, Cuboid.cuboidSelectComparator);
    }

    public Set<Long> getPotentialChildren(long parent) {

        if (parent != Cuboid.getBaseCuboid(cubeDesc).getId() && !Cuboid.isValid(cubeDesc, parent)) {
            throw new IllegalStateException();
        }

        HashSet<Long> set = Sets.newHashSet();
        if (Long.bitCount(parent) == 1) {
            //do not aggregate apex cuboid
            return set;
        }

        if (parent == Cuboid.getBaseCuboidId(cubeDesc)) {
            //base cuboid is responsible for spawning each agg group's root
            for (AggregationGroup agg : cubeDesc.getAggregationGroups()) {
                long partialCubeFullMask = agg.getPartialCubeFullMask();
                if (partialCubeFullMask != parent && Cuboid.isValid(agg, partialCubeFullMask)) {
                    set.add(partialCubeFullMask);
                }
            }
        }

        for (AggregationGroup agg : Cuboid.getValidAggGroupForCuboid(cubeDesc, parent)) {

            //normal dim section
            for (long normalDimMask : agg.getNormalDims()) {
                long common = parent & normalDimMask;
                long temp = parent ^ normalDimMask;
                if (common != 0 && Cuboid.isValid(agg, temp)) {
                    set.add(temp);
                }
            }

            for (AggregationGroup.HierarchyMask hierarchyMask : agg.getHierarchyMasks()) {
                for (int i = hierarchyMask.allMasks.length - 1; i >= 0; i--) {
                    if ((parent & hierarchyMask.allMasks[i]) == hierarchyMask.allMasks[i]) {
                        if ((agg.getJointDimsMask() & hierarchyMask.dims[i]) == 0) {
                            if (Cuboid.isValid(agg, parent ^ hierarchyMask.dims[i])) {
                                //only when the hierarchy dim is not among joints
                                set.add(parent ^ hierarchyMask.dims[i]);
                            }
                        }
                        break;//if hierarchyMask 111 is matched, won't check 110 or 100
                    }
                }
            }

            //joint dim section
            for (long joint : agg.getJoints()) {
                if ((parent & joint) == joint) {
                    if (Cuboid.isValid(agg, parent ^ joint)) {
                        set.add(parent ^ joint);
                    }
                }
            }

        }

        return set;
    }

    public int getCuboidCount() {
        return getCuboidCount(Cuboid.getBaseCuboidId(cubeDesc));
    }

    private int getCuboidCount(long cuboidId) {
        int r = 1;
        for (Long child : getSpanningCuboid(cuboidId)) {
            r += getCuboidCount(child);
        }
        return r;
    }

    public List<Long> getSpanningCuboid(long cuboid) {
        if (cuboid > max || cuboid < 0) {
            throw new IllegalArgumentException("Cuboid " + cuboid + " is out of scope 0-" + max);
        }

        List<Long> result = cache.get(cuboid);
        if (result != null) {
            return result;
        }

        result = Lists.newArrayList();
        Set<Long> potentials = getPotentialChildren(cuboid);
        for (Long potential : potentials) {
            if (getParent(potential) == cuboid) {
                result.add(potential);
            }
        }

        cache.put(cuboid, result);
        return result;
    }

    public int getCardinality(long cuboid) {
        if (cuboid > max || cuboid < 0) {
            throw new IllegalArgumentException("Cubiod " + cuboid + " is out of scope 0-" + max);
        }

        return Long.bitCount(cuboid);
    }

    public List<Long> getAllCuboidIds() {
        final long baseCuboidId = Cuboid.getBaseCuboidId(cubeDesc);
        List<Long> result = Lists.newArrayList();
        getSubCuboidIds(baseCuboidId, result);
        return result;
    }

    private void getSubCuboidIds(long parentCuboidId, List<Long> result) {
        result.add(parentCuboidId);
        for (Long cuboidId : getSpanningCuboid(parentCuboidId)) {
            getSubCuboidIds(cuboidId, result);
        }
    }

    public List<List<Long>> getCuboidsByLayer() {
        if (cuboidsByLayer != null) {
            return cuboidsByLayer;
        }

        int totalNum = 0;
        int layerNum = cubeDesc.getBuildLevel();
        cuboidsByLayer = Lists.newArrayList();

        cuboidsByLayer.add(Collections.singletonList(Cuboid.getBaseCuboidId(cubeDesc)));
        totalNum++;

        for (int i = 1; i <= layerNum; i++) {
            List<Long> lastLayer = cuboidsByLayer.get(i - 1);
            List<Long> newLayer = Lists.newArrayList();
            for (Long parent : lastLayer) {
                newLayer.addAll(getSpanningCuboid(parent));
            }
            cuboidsByLayer.add(newLayer);
            totalNum += newLayer.size();
        }

        int size = getAllCuboidIds().size();
        Preconditions.checkState(totalNum == size, "total Num: " + totalNum + " actual size: " + size);
        return cuboidsByLayer;
    }
}
