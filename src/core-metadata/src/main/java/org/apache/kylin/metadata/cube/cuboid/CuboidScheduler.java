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

import java.io.Serializable;
import java.math.BigInteger;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.lang3.NotImplementedException;
import org.apache.kylin.metadata.cube.model.IndexPlan;
import org.apache.kylin.metadata.cube.model.RuleBasedIndex;

import org.apache.kylin.guava30.shaded.common.collect.Lists;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.val;

/**
 * Defines a cuboid tree, rooted by the base cuboid. A parent cuboid generates its child cuboids.
 */
public abstract class CuboidScheduler implements Serializable {

    public static CuboidScheduler getInstance(IndexPlan indexPlan, RuleBasedIndex ruleBasedIndex, boolean skipAll) {
        if (ruleBasedIndex.getSchedulerVersion() == 1) {
            return new KECuboidSchedulerV1(indexPlan, ruleBasedIndex, skipAll);
        } else if (ruleBasedIndex.getSchedulerVersion() == 2) {
            return new KECuboidSchedulerV2(indexPlan, ruleBasedIndex, skipAll);
        }
        throw new NotImplementedException("Not Support version " + ruleBasedIndex.getSchedulerVersion());
    }

    public static CuboidScheduler getInstance(IndexPlan indexPlan, RuleBasedIndex ruleBasedIndex) {
        return getInstance(indexPlan, ruleBasedIndex, false);
    }

    // ============================================================================

    protected final IndexPlan indexPlan;
    protected final RuleBasedIndex ruleBasedAggIndex;

    protected CuboidScheduler(final IndexPlan indexPlan, RuleBasedIndex ruleBasedAggIndex) {
        this.indexPlan = indexPlan;
        this.ruleBasedAggIndex = ruleBasedAggIndex == null ? indexPlan.getRuleBasedIndex() : ruleBasedAggIndex;
    }

    /**
     * Returns all cuboids on the tree.
     */
    public abstract List<ColOrder> getAllColOrders();

    /**
     * Returns the number of all cuboids.
     */
    public abstract int getCuboidCount();

    public abstract void validateOrder();

    public abstract void updateOrder();

    /**
     * optional
     */
    public abstract List<ColOrder> calculateCuboidsForAggGroup(NAggregationGroup agg);

    // ============================================================================

    public IndexPlan getIndexPlan() {
        return indexPlan;
    }

    public long getAggGroupCombinationSize() {
        return indexPlan.getConfig().getCubeAggrGroupMaxCombination();
    }

    protected ColOrder extractDimAndMeaFromBigInt(BigInteger bigInteger) {
        val allDims = ruleBasedAggIndex.getDimensions();
        val allMeas = ruleBasedAggIndex.getMeasures();
        return extractDimAndMeaFromBigInt(allDims, allMeas, bigInteger);
    }

    protected ColOrder extractDimAndMeaFromBigInt(List<Integer> allDims, List<Integer> allMeas, BigInteger bigInteger) {
        val dims = Lists.<Integer> newArrayList();
        val meas = Lists.<Integer> newArrayList();

        int size = allDims.size() + allMeas.size();

        for (int i = 0; i < size; i++) {
            int shift = size - i - 1;
            if (bigInteger.testBit(shift)) {
                if (i >= allDims.size()) {
                    meas.add(allMeas.get(i - allDims.size()));
                } else {
                    dims.add(allDims.get(i));
                }
            }
        }

        return new ColOrder(dims, meas);
    }

    @Data
    @AllArgsConstructor
    public static class ColOrder {
        private List<Integer> dimensions;
        private List<Integer> measures;

        public List<Integer> toList() {
            return Stream.concat(dimensions.stream(), measures.stream()).collect(Collectors.toList());
        }

    }
}
