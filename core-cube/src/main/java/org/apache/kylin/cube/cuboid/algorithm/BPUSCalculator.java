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

package org.apache.kylin.cube.cuboid.algorithm;

import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kylin.shaded.com.google.common.collect.ImmutableMap;
import org.apache.kylin.shaded.com.google.common.collect.Maps;
import org.apache.kylin.shaded.com.google.common.collect.Sets;

/**
 * Calculate the benefit based on Benefit Per Unit Space.
 */
public class BPUSCalculator implements BenefitPolicy {

    private static Logger logger = LoggerFactory.getLogger(BPUSCalculator.class);

    protected final CuboidStats cuboidStats;
    protected final ImmutableMap<Long, Long> initCuboidAggCostMap;
    protected final Map<Long, Long> processCuboidAggCostMap;

    public BPUSCalculator(CuboidStats cuboidStats) {
        this.cuboidStats = cuboidStats;
        this.initCuboidAggCostMap = ImmutableMap.copyOf(initCuboidAggCostMap());
        this.processCuboidAggCostMap = Maps.newHashMap(initCuboidAggCostMap);
    }

    protected BPUSCalculator(CuboidStats cuboidStats, ImmutableMap<Long, Long> initCuboidAggCostMap) {
        this.cuboidStats = cuboidStats;
        this.initCuboidAggCostMap = initCuboidAggCostMap;
        this.processCuboidAggCostMap = Maps.newHashMap(initCuboidAggCostMap);
    }

    private Map<Long, Long> initCuboidAggCostMap() {
        Map<Long, Long> cuboidAggCostMap = Maps.newHashMap();
        //Initialize stats for mandatory cuboids
        for (Long cuboid : cuboidStats.getAllCuboidsForMandatory()) {
            if (getCuboidCost(cuboid) != null) {
                cuboidAggCostMap.put(cuboid, getCuboidCost(cuboid));
            }
        }

        //Initialize stats for selection cuboids
        long baseCuboidCost = getCuboidCost(cuboidStats.getBaseCuboid());
        for (Long cuboid : cuboidStats.getAllCuboidsForSelection()) {
            long leastCost = baseCuboidCost;
            for (Map.Entry<Long, Long> cuboidTargetEntry : cuboidAggCostMap.entrySet()) {
                if ((cuboid | cuboidTargetEntry.getKey()) == cuboidTargetEntry.getKey()) {
                    if (leastCost > cuboidTargetEntry.getValue()) {
                        leastCost = cuboidTargetEntry.getValue();
                    }
                }
            }
            cuboidAggCostMap.put(cuboid, leastCost);
        }
        return cuboidAggCostMap;
    }

    @Override
    public CuboidBenefitModel.BenefitModel calculateBenefit(long cuboid, Set<Long> selected) {
        double totalCostSaving = 0;
        int benefitCount = 0;
        for (Long descendant : cuboidStats.getAllDescendants(cuboid)) {
            if (!selected.contains(descendant)) {
                double costSaving = getCostSaving(descendant, cuboid);
                if (costSaving > 0) {
                    totalCostSaving += costSaving;
                    benefitCount++;
                }
            }
        }

        double spaceCost = calculateSpaceCost(cuboid);
        double benefitPerUnitSpace = totalCostSaving / spaceCost;
        return new CuboidBenefitModel.BenefitModel(benefitPerUnitSpace, benefitCount);
    }

    @Override
    public CuboidBenefitModel.BenefitModel calculateBenefitTotal(Set<Long> cuboidsToAdd, Set<Long> selected) {
        Set<Long> selectedInner = Sets.newHashSet(selected);
        Map<Long, Long> cuboidAggCostMapCopy = Maps.newHashMap(processCuboidAggCostMap);
        for (Long cuboid : cuboidsToAdd) {
            selectedInner.add(cuboid);
            propagateAggregationCost(cuboid, selectedInner, cuboidAggCostMapCopy);
        }
        double totalCostSaving = 0;
        int benefitCount = 0;
        for (Map.Entry<Long, Long> entry : cuboidAggCostMapCopy.entrySet()) {
            if (entry.getValue() < processCuboidAggCostMap.get(entry.getKey())) {
                totalCostSaving += processCuboidAggCostMap.get(entry.getKey()) - entry.getValue();
                benefitCount++;
            }
        }

        double benefitPerUnitSpace = totalCostSaving;
        return new CuboidBenefitModel.BenefitModel(benefitPerUnitSpace, benefitCount);
    }

    protected double getCostSaving(long descendant, long cuboid) {
        long cuboidCost = getCuboidCost(cuboid);
        long descendantAggCost = getCuboidAggregationCost(descendant);
        return (double) descendantAggCost - cuboidCost;
    }

    protected Long getCuboidCost(long cuboid) {
        return cuboidStats.getCuboidCount(cuboid);
    }

    private long getCuboidAggregationCost(long cuboid) {
        return processCuboidAggCostMap.get(cuboid);
    }

    @Override
    public boolean ifEfficient(CuboidBenefitModel best) {
        if (best.getBenefit() < getMinBenefitRatio()) {
            logger.info(String.format(Locale.ROOT, "The recommended cuboid %s doesn't meet minimum benifit ratio %f",
                    best, getMinBenefitRatio()));
            return false;
        }
        return true;
    }

    public double getMinBenefitRatio() {
        return cuboidStats.getBpusMinBenefitRatio();
    }

    @Override
    public void propagateAggregationCost(long cuboid, Set<Long> selected) {
        propagateAggregationCost(cuboid, selected, processCuboidAggCostMap);
    }

    public void propagateAggregationCost(long cuboid, Set<Long> selected, Map<Long, Long> processCuboidAggCostMap) {
        long aggregationCost = getCuboidCost(cuboid);
        Set<Long> childrenCuboids = cuboidStats.getAllDescendants(cuboid);
        for (Long child : childrenCuboids) {
            if (!selected.contains(child) && (aggregationCost < getCuboidAggregationCost(child))) {
                processCuboidAggCostMap.put(child, aggregationCost);
            }
        }
    }

    /**
     * Return the space cost of building a cuboid.
     *
     */
    public double calculateSpaceCost(long cuboid) {
        return cuboidStats.getCuboidCount(cuboid);
    }

    @Override
    public BenefitPolicy getInstance() {
        return new BPUSCalculator(this.cuboidStats, this.initCuboidAggCostMap);
    }
}