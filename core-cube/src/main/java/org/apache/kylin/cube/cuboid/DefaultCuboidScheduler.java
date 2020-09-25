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

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import javax.annotation.Nullable;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.cube.model.AggregationGroup;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.TooManyCuboidException;

import org.apache.kylin.shaded.com.google.common.base.Predicate;
import org.apache.kylin.shaded.com.google.common.collect.Iterators;
import org.apache.kylin.shaded.com.google.common.collect.Lists;
import org.apache.kylin.shaded.com.google.common.collect.Maps;
import org.apache.kylin.shaded.com.google.common.collect.Sets;

public class DefaultCuboidScheduler extends CuboidScheduler {
    private final long max;
    private final Set<Long> allCuboidIds;
    private final Map<Long, List<Long>> parent2child;

    public DefaultCuboidScheduler(CubeDesc cubeDesc) {
        super(cubeDesc);

        int size = this.cubeDesc.getRowkey().getRowKeyColumns().length;
        this.max = (long) Math.pow(2, size) - 1;

        Pair<Set<Long>, Map<Long, List<Long>>> pair = buildTreeBottomUp();
        this.allCuboidIds = pair.getFirst();
        this.parent2child = pair.getSecond();
    }

    @Override
    public int getCuboidCount() {
        return allCuboidIds.size();
    }

    @Override
    public List<Long> getSpanningCuboid(long cuboid) {
        if (cuboid > max || cuboid < 0) {
            throw new IllegalArgumentException("Cuboid " + cuboid + " is out of scope 0-" + max);
        }

        List<Long> spanning = parent2child.get(cuboid);
        if (spanning == null) {
            return Collections.EMPTY_LIST;
        }
        return spanning;
    }

    @Override
    public Set<Long> getAllCuboidIds() {
        return Sets.newHashSet(allCuboidIds);
    }

    @Override
    public boolean isValid(long requestCuboid) {
        return allCuboidIds.contains(requestCuboid);
    }

    /** Returns a valid cuboid that best matches the request cuboid. */
    @Override
    public long findBestMatchCuboid(long cuboid) {
        return findBestMatchCuboid1(cuboid);
    }

    long findBestMatchCuboid1(long cuboid) {
        if (isValid(cuboid)) {
            return cuboid;
        }

        List<Long> onTreeCandidates = Lists.newArrayList();
        for (AggregationGroup agg : cubeDesc.getAggregationGroups()) {
            Long candidate = agg.translateToOnTreeCuboid(cuboid);
            if (candidate != null) {
                onTreeCandidates.add(candidate);
            }
        }

        if (onTreeCandidates.size() == 0) {
            return getBaseCuboidId();
        }

        long onTreeCandi = Collections.min(onTreeCandidates, Cuboid.cuboidSelectComparator);
        if (isValid(onTreeCandi)) {
            return onTreeCandi;
        }

        return doFindBestMatchCuboid1(onTreeCandi);
    }

    private long doFindBestMatchCuboid1(long cuboid) {
        long parent = getOnTreeParent(cuboid);
        while (parent > 0) {
            if (cubeDesc.getAllCuboids().contains(parent)) {
                break;
            }
            parent = getOnTreeParent(parent);
        }

        if (parent <= 0) {
            throw new IllegalStateException("Can't find valid parent for Cuboid " + cuboid);
        }
        return parent;
    }

    private long getOnTreeParent(long child) {
        Collection<Long> candidates = getOnTreeParents(child);
        if (candidates == null || candidates.isEmpty()) {
            return -1;
        }
        return Collections.min(candidates, Cuboid.cuboidSelectComparator);
    }

    private Set<Long> getOnTreeParents(long child) {
        List<AggregationGroup> aggrs = Lists.newArrayList();
        for (AggregationGroup agg : cubeDesc.getAggregationGroups()) {
            if (agg.isOnTree(child)) {
                aggrs.add(agg);
            }
        }

        return getOnTreeParents(child, aggrs);
    }

    /**
     * Collect cuboid from bottom up, considering all factor including black list
     * Build tree steps:
     * 1. Build tree from bottom up considering dim capping
     * 2. Kick out blacked-out cuboids from the tree
     * 3. Make sure all the cuboids have proper "parent", if not add it to the tree.
     *    Direct parent is not necessary, can jump *forward* steps to find in-direct parent.
     *    For example, forward = 1, grandparent can also be the parent. Only if both parent
     *    and grandparent are missing, add grandparent to the tree.
     * @return Cuboid collection
     */
    protected Pair<Set<Long>, Map<Long, List<Long>>> buildTreeBottomUp() {
        int forward = cubeDesc.getParentForward();
        KylinConfig config = cubeDesc.getConfig();

        Set<Long> cuboidHolder = new HashSet<>();

        // build tree structure
        Set<Long> children = getLowestCuboids();
        long maxCombination = config.getCubeAggrGroupMaxCombination() * 10;
        maxCombination = maxCombination < 0 ? Long.MAX_VALUE : maxCombination;
        while (!children.isEmpty()) {
            cuboidHolder.addAll(children);
            if (cuboidHolder.size() > maxCombination) {
                throw new IllegalStateException("Too many cuboids for the cube. Cuboid combination reached "
                    + cuboidHolder.size() + " and limit is " + maxCombination + ". Abort calculation.");
            }
            children = getOnTreeParentsByLayer(children);
        }
        cuboidHolder.add(Cuboid.getBaseCuboidId(cubeDesc));

        // kick off blacked
        cuboidHolder = Sets.newHashSet(Iterators.filter(cuboidHolder.iterator(), new Predicate<Long>() {
            @Override
            public boolean apply(@Nullable Long cuboidId) {
                return !cubeDesc.isBlackedCuboid(cuboidId);
            }
        }));

        // fill padding cuboids
        Map<Long, List<Long>> parent2Child = Maps.newHashMap();
        Queue<Long> cuboidScan = new ArrayDeque<>();
        cuboidScan.addAll(cuboidHolder);
        while (!cuboidScan.isEmpty()) {
            long current = cuboidScan.poll();
            long parent = getParentOnPromise(current, cuboidHolder, forward);

            if (parent > 0) {
                if (!cuboidHolder.contains(parent)) {
                    cuboidScan.add(parent);
                    cuboidHolder.add(parent);
                }
                if (parent2Child.containsKey(parent)) {
                    parent2Child.get(parent).add(current);
                } else {
                    parent2Child.put(parent, Lists.newArrayList(current));
                }
            }
        }

        return Pair.newPair(cuboidHolder, parent2Child);
    }

    private long getParentOnPromise(long child, Set<Long> coll, int forward) {
        long parent = getOnTreeParent(child);
        if (parent < 0) {
            return -1;
        }

        if (coll.contains(parent) || forward == 0) {
            return parent;
        }

        return getParentOnPromise(parent, coll, forward - 1);
    }

    /**
     * Get all parent for children cuboids, considering dim cap.
     * @param children children cuboids
     * @return all parents cuboids
     */
    private Set<Long> getOnTreeParentsByLayer(Collection<Long> children) {
        Set<Long> parents = new HashSet<>();
        for (long child : children) {
            parents.addAll(getOnTreeParents(child));
        }
        parents = Sets.newHashSet(Iterators.filter(parents.iterator(), new Predicate<Long>() {
            @Override
            public boolean apply(@Nullable Long cuboidId) {
                if (cuboidId == Cuboid.getBaseCuboidId(cubeDesc)) {
                    return true;
                }

                for (AggregationGroup agg : cubeDesc.getAggregationGroups()) {
                    if (agg.isOnTree(cuboidId) && checkDimCap(agg, cuboidId)) {
                        return true;
                    }
                }

                return false;
            }
        }));
        return parents;
    }

    /**
     * Get lowest (not Cube building level) Cuboids for every Agg group
     * @return lowest level cuboids
     */
    private Set<Long> getLowestCuboids() {
        return getOnTreeParents(0L, cubeDesc.getAggregationGroups());
    }

    private Set<Long> getOnTreeParents(long child, Collection<AggregationGroup> groups) {
        Set<Long> parentCandidate = new HashSet<>();

        if (child == Cuboid.getBaseCuboidId(cubeDesc)) {
            return parentCandidate;
        }

        for (AggregationGroup agg : groups) {
            if (child == agg.getPartialCubeFullMask()) {
                parentCandidate.add(Cuboid.getBaseCuboidId(cubeDesc));
                return parentCandidate;
            }
            parentCandidate.addAll(getOnTreeParents(child, agg));
        }

        return parentCandidate;
    }

    /**
     * Get all valid cuboids for agg group, ignoring padding
     * @param agg agg group
     * @return cuboidId list
     */
    public Set<Long> calculateCuboidsForAggGroup(AggregationGroup agg) {
        Set<Long> cuboidHolder = new HashSet<>();

        // build tree structure
        Set<Long> children = getLowestCuboids(agg);
        while (!children.isEmpty()) {
            if (cuboidHolder.size() > cubeDesc.getConfig().getCubeAggrGroupMaxCombination()) {
                throw new TooManyCuboidException("Holder size larger than kylin.cube.aggrgroup.max-combination");
            }
            cuboidHolder.addAll(children);
            children = getOnTreeParentsByLayer(children, agg);
        }

        return Sets.newHashSet(Iterators.filter(cuboidHolder.iterator(), new Predicate<Long>() {
            @Override
            public boolean apply(@Nullable Long cuboidId) {
                return !cubeDesc.isBlackedCuboid(cuboidId);
            }
        }));
    }

    private Set<Long> getOnTreeParentsByLayer(Collection<Long> children, final AggregationGroup agg) {
        Set<Long> parents = new HashSet<>();
        for (long child : children) {
            parents.addAll(getOnTreeParents(child, agg));
        }
        parents = Sets.newHashSet(Iterators.filter(parents.iterator(), new Predicate<Long>() {
            @Override
            public boolean apply(@Nullable Long cuboidId) {
                return checkDimCap(agg, cuboidId);
            }
        }));
        return parents;
    }

    private Set<Long> getLowestCuboids(AggregationGroup agg) {
        return getOnTreeParents(0L, agg);
    }

    private Set<Long> getOnTreeParents(long child, AggregationGroup agg) {
        Set<Long> parentCandidate = new HashSet<>();

        long tmpChild = child;
        if (tmpChild == agg.getPartialCubeFullMask()) {
            return parentCandidate;
        }

        if (agg.getMandatoryColumnMask() != 0L) {
            if (agg.isMandatoryOnlyValid()) {
                if (fillBit(tmpChild, agg.getMandatoryColumnMask(), parentCandidate)) {
                    return parentCandidate;
                }
            } else {
                tmpChild |= agg.getMandatoryColumnMask();
            }
        }

        for (Long normal : agg.getNormalDims()) {
            fillBit(tmpChild, normal, parentCandidate);
        }

        for (Long joint : agg.getJoints()) {
            fillBit(tmpChild, joint, parentCandidate);
        }

        for (AggregationGroup.HierarchyMask hierarchy : agg.getHierarchyMasks()) {
            for (long mask : hierarchy.allMasks) {
                if (fillBit(tmpChild, mask, parentCandidate)) {
                    break;
                }
            }
        }

        return parentCandidate;
    }

    private boolean fillBit(long origin, long other, Set<Long> coll) {
        if ((origin & other) != other) {
            coll.add(origin | other);
            return true;
        }
        return false;
    }

    private boolean checkDimCap(AggregationGroup agg, long cuboidID) {
        int dimCap = agg.getDimCap();
        if (dimCap <= 0)
            return true;

        return Long.bitCount(cuboidID) <= dimCap;
    }

    long findBestMatchCuboid2(long cuboid) {
        long bestParent = doFindBestMatchCuboid2(cuboid, Cuboid.getBaseCuboidId(cubeDesc));
        if (bestParent < -1) {
            throw new IllegalStateException("Cannot find the parent of the cuboid:" + cuboid);
        }
        return bestParent;
    }

    private long doFindBestMatchCuboid2(long cuboid, long parent) {
        if (!canDerive(cuboid, parent)) {
            return -1;
        }
        List<Long> children = parent2child.get(parent);
        List<Long> candidates = Lists.newArrayList();
        if (children != null) {
            for (long child : children) {
                long candidate = doFindBestMatchCuboid2(cuboid, child);
                if (candidate > 0) {
                    candidates.add(candidate);
                }
            }
        }
        if (candidates.isEmpty()) {
            candidates.add(parent);
        }

        return Collections.min(candidates, Cuboid.cuboidSelectComparator);
    }

    private boolean canDerive(long cuboidId, long parentCuboid) {
        return (cuboidId & ~parentCuboid) == 0;
    }

}
