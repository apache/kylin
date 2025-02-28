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

import static org.apache.kylin.common.exception.code.ErrorCodeServer.RULE_BASED_INDEX_METADATA_INCONSISTENT;

import java.math.BigInteger;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nullable;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.exception.OutOfMaxCombinationException;
import org.apache.kylin.common.exception.code.ErrorCodeServer;
import org.apache.kylin.common.util.ThreadUtil;
import org.apache.kylin.guava30.shaded.common.base.Predicate;
import org.apache.kylin.guava30.shaded.common.collect.Iterators;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.apache.kylin.metadata.cube.model.IndexPlan;
import org.apache.kylin.metadata.cube.model.RuleBasedIndex;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class KECuboidSchedulerV1 extends CuboidScheduler {

    public static final String INDEX_SCHEDULER_KEY = "kylin.index.rule-scheduler-data";
    public static final String METADATA_INCONSISTENT_ERROR_MSG_PATTERN = "Index metadata might be inconsistent. Please try refreshing all segments in the following model({}/{}), Reason: {}, critical stackTrace:\n{}";
    private final BigInteger max;
    private final int measureSize;
    private final boolean isBaseCuboidValid;
    private transient final Set<CuboidBigInteger> allCuboidIds;

    private transient final SetCreator newHashSet = HashSet::new;

    KECuboidSchedulerV1(IndexPlan indexPlan, RuleBasedIndex ruleBasedAggIndex, boolean skipAllCuboids) {
        super(indexPlan, ruleBasedAggIndex);

        this.max = ruleBasedAggIndex.getFullMask();
        this.measureSize = ruleBasedAggIndex.getMeasures().size();
        this.isBaseCuboidValid = ruleBasedAggIndex.getIndexPlan().getConfig().isBaseCuboidAlwaysValid();
        if (skipAllCuboids) {
            this.allCuboidIds = new OrderedSet<>();
            return;
        }

        // handle nRuleBasedCuboidDesc has 0 dimensions
        if (max.bitCount() == 0) {
            allCuboidIds = new OrderedSet<>();
        } else {
            SetCreator newCuboidSet = OrderedSet::new;
            this.allCuboidIds = buildTreeBottomUp(newCuboidSet);
        }
    }

    @Override
    public int getCuboidCount() {
        return allCuboidIds.size();
    }

    @Override
    public void validateOrder() {
        List<CuboidBigInteger> newSortingResult = ((OrderedSet) allCuboidIds).getSortedList();
        val data = indexPlan.getOverrideProps().get(INDEX_SCHEDULER_KEY);
        List<BigInteger> oldSortingResult;
        if (StringUtils.isEmpty(data)) {
            Set<CuboidBigInteger> oldAllCuboidIds = Sets.newHashSet();
            if (max.bitCount() > 0) {
                childParents.clear();
                oldAllCuboidIds = buildTreeBottomUp(newHashSet);
            }
            oldSortingResult = Sets.newHashSet(oldAllCuboidIds).stream().map(CuboidBigInteger::getDimMeas)
                    .collect(Collectors.toList());
        } else {
            oldSortingResult = Stream.of(StringUtils.split(data, ",")).map(BigInteger::new)
                    .collect(Collectors.toList());
        }
        List<BigInteger> newSortingOrder = newSortingResult.stream().map(CuboidBigInteger::getDimMeas)
                .collect(Collectors.toList());
        if (!Objects.equals(newSortingOrder, oldSortingResult)) {
            log.error(METADATA_INCONSISTENT_ERROR_MSG_PATTERN, indexPlan.getProject(), indexPlan.getModelAlias(),
                    RULE_BASED_INDEX_METADATA_INCONSISTENT, ThreadUtil.getKylinStackTrace());
            log.debug("Set difference new:{}, old:{}", newSortingOrder, oldSortingResult);
        }

    }

    @Override
    public void updateOrder() {
        val newSortingResult = ((OrderedSet<CuboidBigInteger>) allCuboidIds).getSortedList();
        indexPlan.getOverrideProps().put(KECuboidSchedulerV1.INDEX_SCHEDULER_KEY,
                StringUtils.join(newSortingResult, ","));
    }

    @Override
    public List<ColOrder> getAllColOrders() {
        return ((OrderedSet<CuboidBigInteger>) allCuboidIds).getSortedList().stream()
                .map(c -> extractDimAndMeaFromBigInt(c.getDimMeas())).collect(Collectors.toList());
    }

    private final Map<CuboidBigInteger, Set<CuboidBigInteger>> childParents = Maps.newConcurrentMap();

    private Set<CuboidBigInteger> getOnTreeParents(CuboidBigInteger child, SetCreator setCreatorFunc) {
        if (childParents.containsKey(child)) {
            return childParents.get(child);
        }
        Set<CuboidBigInteger> parentCandidate = setCreatorFunc.create();
        BigInteger childBits = child.getDimMeas();

        if (isBaseCuboidValid && childBits.equals(ruleBasedAggIndex.getFullMask())) {
            return parentCandidate;
        }

        for (NAggregationGroup agg : ruleBasedAggIndex.getAggregationGroups()) {
            if (childBits.equals(agg.getPartialCubeFullMask()) && isBaseCuboidValid) {
                parentCandidate.add(new CuboidBigInteger(ruleBasedAggIndex.getFullMask(), measureSize));
            } else if (childBits.equals(BigInteger.ZERO) || agg.isOnTree(childBits)) {
                parentCandidate.addAll(getOnTreeParents(child, agg, setCreatorFunc));
            }
        }

        childParents.put(child, parentCandidate);
        return parentCandidate;
    }

    private interface SetCreator {
        Set<CuboidBigInteger> create();
    }

    /**
     * Collect cuboid from bottom up, considering all factor including black list
     * Build tree steps:
     * 1. Build tree from bottom up considering dim capping
     * 2. Kick out blacked-out cuboids from the tree
     *
     * @return Cuboid collection
     */
    private Set<CuboidBigInteger> buildTreeBottomUp(SetCreator setCreatorFunc) {
        Set<CuboidBigInteger> cuboidHolder = setCreatorFunc.create();

        // build tree structure
        long maxCombination = getAggGroupCombinationSize() * 10;
        maxCombination = maxCombination < 0 ? Integer.MAX_VALUE : maxCombination;
        Set<CuboidBigInteger> children = getOnTreeParentsByLayer(Sets.newHashSet(new CuboidBigInteger(BigInteger.ZERO)),
                setCreatorFunc, maxCombination); // lowest level cuboids
        while (!children.isEmpty()) {
            if (cuboidHolder.size() + children.size() > maxCombination) {
                throw new OutOfMaxCombinationException(ErrorCodeServer.OUT_OF_MAX_DIM_COMBINATION, maxCombination);
            }
            cuboidHolder.addAll(children);
            children = getOnTreeParentsByLayer(children, setCreatorFunc, maxCombination);
        }

        if (isBaseCuboidValid) {
            cuboidHolder.add(new CuboidBigInteger(ruleBasedAggIndex.getFullMask(), measureSize));
        }

        return cuboidHolder;
    }

    /**
     * Get all parent for children cuboids, considering dim cap.
     *
     * @param children children cuboids
     * @return all parents cuboids
     */
    private Set<CuboidBigInteger> getOnTreeParentsByLayer(Collection<CuboidBigInteger> children,
            SetCreator setCreatorFunc, long maxCombination) {
        Set<CuboidBigInteger> parents = setCreatorFunc.create();
        for (CuboidBigInteger child : children) {
            parents.addAll(getOnTreeParents(child, setCreatorFunc));
        }

        //debugPrint(parents, "parents");
        val filteredParents = Iterators.filter(parents.iterator(), new Predicate<CuboidBigInteger>() {
            private int cuboidCount = 0;

            @Override
            public boolean apply(@Nullable CuboidBigInteger cuboidId) {
                if (cuboidId == null) {
                    return false;
                }
                if (cuboidCount > maxCombination) {
                    throw new OutOfMaxCombinationException(ErrorCodeServer.OUT_OF_MAX_DIM_COMBINATION, maxCombination);
                }

                BigInteger cuboidBits = cuboidId.getDimMeas();

                if (cuboidBits.equals(ruleBasedAggIndex.getFullMask()) && isBaseCuboidValid) {
                    cuboidCount++;
                    return true;
                }

                for (NAggregationGroup agg : ruleBasedAggIndex.getAggregationGroups()) {
                    if (agg.isOnTree(cuboidBits) && agg.checkDimCap(cuboidBits)) {
                        cuboidCount++;
                        return true;
                    }
                }

                return false;
            }
        });
        parents = setCreatorFunc.create();
        while (filteredParents.hasNext()) {
            parents.add(filteredParents.next());
        }

        //debugPrint(parents, "parents-dimcapped");
        return parents;
    }

    /**
     * Get all valid cuboids for agg group, ignoring padding
     *
     * @param agg agg group
     * @return cuboidId list
     */
    @Override
    public List<ColOrder> calculateCuboidsForAggGroup(NAggregationGroup agg) {
        Set<CuboidBigInteger> cuboidHolder = newHashSet.create();

        // build tree structure
        Set<CuboidBigInteger> children = getOnTreeParentsByLayer(Sets.newHashSet(new CuboidBigInteger(BigInteger.ZERO)),
                agg, newHashSet); // lowest level cuboids
        while (!children.isEmpty()) {
            if (cuboidHolder.size() + children.size() > getAggGroupCombinationSize()) {
                throw new OutOfMaxCombinationException(ErrorCodeServer.OUT_OF_MAX_DIM_COMBINATION,
                        getAggGroupCombinationSize());
            }
            cuboidHolder.addAll(children);
            children = getOnTreeParentsByLayer(children, agg, newHashSet);
        }

        return cuboidHolder.stream().map(c -> extractDimAndMeaFromBigInt(c.getDimMeas())).collect(Collectors.toList());
    }

    private Set<CuboidBigInteger> getOnTreeParentsByLayer(Collection<CuboidBigInteger> children,
            final NAggregationGroup agg, SetCreator setCreatorFunc) {
        Set<CuboidBigInteger> parents = setCreatorFunc.create();
        for (CuboidBigInteger child : children) {
            parents.addAll(getOnTreeParents(child, agg, setCreatorFunc));
        }
        val filteredParent = Iterators.filter(parents.iterator(), cuboidId -> {
            if (cuboidId == null)
                return false;
            return agg.checkDimCap(cuboidId.getDimMeas());
        });
        parents = setCreatorFunc.create();
        while (filteredParent.hasNext()) {
            parents.add(filteredParent.next());
        }
        return parents;
    }

    private Set<CuboidBigInteger> getOnTreeParents(CuboidBigInteger child, NAggregationGroup agg,
            SetCreator setCreatorFunc) {
        Set<CuboidBigInteger> parentCandidate = setCreatorFunc.create();

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
