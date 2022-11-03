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

package org.apache.kylin.metadata.cube.cuboid;

import java.math.BigInteger;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.kylin.common.exception.OutOfMaxCombinationException;
import org.apache.kylin.metadata.cube.model.IndexPlan;
import org.apache.kylin.metadata.cube.model.RuleBasedIndex;

import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class KECuboidSchedulerV2 extends CuboidScheduler {

    private final BigInteger max;
    private final int measureSize;
    private transient final OrderedSet<ColOrder> allColOrders;

    KECuboidSchedulerV2(IndexPlan indexPlan, RuleBasedIndex ruleBasedAggIndex, boolean skipAll) {
        super(indexPlan, ruleBasedAggIndex);

        this.max = ruleBasedAggIndex.getFullMask();
        this.measureSize = ruleBasedAggIndex.getMeasures().size();

        // handle nRuleBasedCuboidDesc has 0 dimensions
        allColOrders = new OrderedSet<>();
        if (max.bitCount() == 0 || skipAll) {
            return;
        }
        long maxCombination = indexPlan.getConfig().getCubeAggrGroupMaxCombination() * 10;
        maxCombination = maxCombination < 0 ? Long.MAX_VALUE : maxCombination;
        if (ruleBasedAggIndex.getBaseLayoutEnabled() == null) {
            ruleBasedAggIndex.setBaseLayoutEnabled(true);
        }
        if (Boolean.TRUE.equals(ruleBasedAggIndex.getBaseLayoutEnabled())) {
            allColOrders.add(new ColOrder(ruleBasedAggIndex.getDimensions(), ruleBasedAggIndex.getMeasures()));
        }
        for (NAggregationGroup agg : ruleBasedAggIndex.getAggregationGroups()) {
            allColOrders.addAll(calculateCuboidsForAggGroup(agg));
            if (allColOrders.size() > maxCombination) {
                throw new OutOfMaxCombinationException(String.format(Locale.ROOT, OUT_OF_MAX_COMBINATION_MSG_FORMAT,
                        allColOrders.size(), maxCombination));
            }
        }
    }

    @Override
    public int getCuboidCount() {
        return allColOrders.size();
    }

    @Override
    public void validateOrder() {
        // do nothing
    }

    @Override
    public void updateOrder() {
        // do nothing
    }

    @Override
    public List<ColOrder> getAllColOrders() {
        return allColOrders.getSortedList();
    }

    /**
     * Get all valid cuboids for agg group, ignoring padding
     *
     * @param agg agg group
     * @return cuboidId list
     */
    @Override
    public List<ColOrder> calculateCuboidsForAggGroup(NAggregationGroup agg) {
        Set<CuboidBigInteger> cuboidHolder = new OrderedSet<>();

        // build tree structure
        Set<CuboidBigInteger> children = getOnTreeParentsByLayer(Sets.newHashSet(new CuboidBigInteger(BigInteger.ZERO)),
                agg); // lowest level cuboids
        while (!children.isEmpty()) {
            if (cuboidHolder.size() + children.size() > indexPlan.getConfig().getCubeAggrGroupMaxCombination()) {
                throw new OutOfMaxCombinationException("Holder size larger than kylin.cube.aggrgroup.max-combination");
            }
            cuboidHolder.addAll(children);
            children = getOnTreeParentsByLayer(children, agg);
        }

        return cuboidHolder.stream().map(c -> {
            val colOrder = extractDimAndMeaFromBigInt(c.getDimMeas());
            colOrder.getDimensions().sort(Comparator.comparingInt(x -> ArrayUtils.indexOf(agg.getIncludes(), x)));
            return colOrder;
        }).collect(Collectors.toList());
    }

    private Set<CuboidBigInteger> getOnTreeParentsByLayer(Collection<CuboidBigInteger> children,
            final NAggregationGroup agg) {
        Set<CuboidBigInteger> parents = new OrderedSet<>();
        for (CuboidBigInteger child : children) {
            parents.addAll(getOnTreeParents(child, agg));
        }
        val filteredParent = Iterators.filter(parents.iterator(), cuboidId -> {
            if (cuboidId == null)
                return false;

            return agg.checkDimCap(cuboidId.getDimMeas());
        });
        parents = new OrderedSet<>();
        while (filteredParent.hasNext()) {
            parents.add(filteredParent.next());
        }
        return parents;
    }

    private Set<CuboidBigInteger> getOnTreeParents(CuboidBigInteger child, NAggregationGroup agg) {
        Set<CuboidBigInteger> parentCandidate = new OrderedSet<>();

        BigInteger tmpChild = child.getDimMeas();
        if (tmpChild.equals(agg.getPartialCubeFullMask())) {
            return parentCandidate;
        }

        if (!agg.getMandatoryColumnMask().equals(agg.getMeasureMask())) {
            if (agg.isMandatoryOnlyValid()) {
                if (fillBit(tmpChild, agg.getMandatoryColumnMask(), parentCandidate)) {
                    return parentCandidate;
                }
            } else {
                tmpChild = tmpChild.or(agg.getMandatoryColumnMask());
            }
        }

        for (BigInteger normal : agg.getNormalDimMeas()) {
            fillBit(tmpChild, normal, parentCandidate);
        }

        for (BigInteger joint : agg.getJoints()) {
            fillBit(tmpChild, joint, parentCandidate);
        }

        for (NAggregationGroup.HierarchyMask hierarchy : agg.getHierarchyMasks()) {
            for (BigInteger mask : hierarchy.getAllMasks()) {
                if (fillBit(tmpChild, mask, parentCandidate)) {
                    break;
                }
            }
        }

        return parentCandidate;
    }

    private boolean fillBit(BigInteger origin, BigInteger other, Set<CuboidBigInteger> coll) {
        // if origin contains does not all elements in other
        if (!(origin.and(other)).equals(other)) {
            coll.add(new CuboidBigInteger(origin.or(other), measureSize));
            return true;
        }
        return false;
    }

}
