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

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.annotation.Nullable;

import org.apache.kylin.cube.model.AggregationGroup;
import org.apache.kylin.cube.model.CubeDesc;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class AggregationGroupScheduler {
    /**
     * Get all valid cuboids for agg group, ignoring padding
     * @param agg agg group
     * @return cuboidId list
     */
    public static List<Long> getCuboidsForAgg(final CubeDesc cubeDesc, AggregationGroup agg) {
        Set<Long> cuboidHolder = new HashSet<>();

        // build tree structure
        Set<Long> children = getLowestCuboids(agg);
        while (!children.isEmpty()) {
            if (cuboidHolder.size() > cubeDesc.getConfig().getCubeAggrGroupMaxCombination()) {
                throw new IllegalStateException("too many combination for the aggregation group");
            }
            cuboidHolder.addAll(children);
            children = getOnTreeParentsByLayer(children, agg);
        }

        return Lists.newArrayList(Iterators.filter(cuboidHolder.iterator(), new Predicate<Long>() {
            @Override
            public boolean apply(@Nullable Long cuboidId) {
                return !cubeDesc.isBlackedCuboid(cuboidId);
            }
        }));
    }

    private static Set<Long> getOnTreeParentsByLayer(Collection<Long> children, final AggregationGroup agg) {
        Set<Long> parents = new HashSet<>();
        for (long child : children) {
            parents.addAll(getOnTreeParents(child, agg));
        }
        parents = Sets.newHashSet(Iterators.filter(parents.iterator(), new Predicate<Long>() {
            @Override
            public boolean apply(@Nullable Long cuboidId) {
                return agg.checkDimCap(cuboidId);
            }
        }));
        return parents;
    }

    private static Set<Long> getLowestCuboids(AggregationGroup agg) {
        return getOnTreeParents(0L, agg);
    }

    public static Set<Long> getOnTreeParents(long child, AggregationGroup agg) {
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

    private static boolean fillBit(long origin, long other, Set<Long> coll) {
        if ((origin & other) != other) {
            coll.add(origin | other);
            return true;
        }
        return false;
    }

}
