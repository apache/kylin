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

package org.apache.kylin.metadata.cube.planner.algorithm;

import java.math.BigInteger;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class BPUSCalculator implements BenefitPolicy {

    protected final LayoutStats layoutStats;
    protected final ImmutableMap<BigInteger, Long> initLayoutAggCostMap;
    protected final Map<BigInteger, Long> processLayoutAggCostMap;

    public BPUSCalculator(LayoutStats layoutStats) {
        this.layoutStats = layoutStats;
        this.initLayoutAggCostMap = ImmutableMap.copyOf(initLayoutAggCostMap());
        this.processLayoutAggCostMap = Maps.newHashMap(initLayoutAggCostMap);
    }

    protected BPUSCalculator(LayoutStats layoutStats, ImmutableMap<BigInteger, Long> initLayoutAggCostMap) {
        this.layoutStats = layoutStats;
        this.initLayoutAggCostMap = initLayoutAggCostMap;
        this.processLayoutAggCostMap = Maps.newHashMap(initLayoutAggCostMap);
    }

    private Map<BigInteger, Long> initLayoutAggCostMap() {
        Map<BigInteger, Long> layoutAggCostMap = Maps.newHashMap();
        //Initialize stats for mandatory layouts
        for (BigInteger layout : layoutStats.getAllLayoutsForMandatory()) {
            if (getLayoutCost(layout) != null) {
                layoutAggCostMap.put(layout, getLayoutCost(layout));
            }
        }

        //Initialize stats for selection layouts
        long baseLayoutCost = getLayoutCost(layoutStats.getBaseLayout());
        for (BigInteger layout : layoutStats.getAllLayoutsForSelection()) {
            long leastCost = baseLayoutCost;
            for (Map.Entry<BigInteger, Long> layoutTargetEntry : layoutAggCostMap.entrySet()) {
                // use the equal to check two value
                if ((layout.or(layoutTargetEntry.getKey())).equals(layoutTargetEntry.getKey())) {
                    if (leastCost > layoutTargetEntry.getValue()) {
                        leastCost = layoutTargetEntry.getValue();
                    }
                }
            }
            layoutAggCostMap.put(layout, leastCost);
        }
        return layoutAggCostMap;
    }

    @Override
    public LayoutBenefitModel.BenefitModel calculateBenefit(BigInteger layout, Set<BigInteger> selected) {
        double totalCostSaving = 0;
        int benefitCount = 0;
        for (BigInteger descendant : layoutStats.getAllDescendants(layout)) {
            if (!selected.contains(descendant)) {
                double costSaving = getCostSaving(descendant, layout);
                if (costSaving > 0) {
                    totalCostSaving += costSaving;
                    benefitCount++;
                }
            }
        }

        double spaceCost = calculateSpaceCost(layout);
        double benefitPerUnitSpace = totalCostSaving / spaceCost;
        return new LayoutBenefitModel.BenefitModel(benefitPerUnitSpace, benefitCount);
    }

    @Override
    public LayoutBenefitModel.BenefitModel calculateBenefitTotal(Set<BigInteger> layoutsToAdd,
            Set<BigInteger> selected) {
        Set<BigInteger> selectedInner = Sets.newHashSet(selected);
        Map<BigInteger, Long> layoutAggCostMapCopy = Maps.newHashMap(processLayoutAggCostMap);
        for (BigInteger layout : layoutsToAdd) {
            selectedInner.add(layout);
            propagateAggregationCost(layout, selectedInner, layoutAggCostMapCopy);
        }
        double totalCostSaving = 0;
        int benefitCount = 0;
        for (Map.Entry<BigInteger, Long> entry : layoutAggCostMapCopy.entrySet()) {
            if (entry.getValue() < processLayoutAggCostMap.get(entry.getKey())) {
                totalCostSaving += processLayoutAggCostMap.get(entry.getKey()) - entry.getValue();
                benefitCount++;
            }
        }

        double benefitPerUnitSpace = totalCostSaving;
        return new LayoutBenefitModel.BenefitModel(benefitPerUnitSpace, benefitCount);
    }

    protected double getCostSaving(BigInteger descendant, BigInteger layout) {
        long layoutCost = getLayoutCost(layout);
        long descendantAggCost = getLayoutAggregationCost(descendant);
        return (double) descendantAggCost - layoutCost;
    }

    protected Long getLayoutCost(BigInteger layout) {
        return layoutStats.getLayoutCount(layout);
    }

    private long getLayoutAggregationCost(BigInteger layout) {
        return processLayoutAggCostMap.get(layout);
    }

    @Override
    public boolean ifEfficient(LayoutBenefitModel best) {
        if (best.getBenefit() < getMinBenefitRatio()) {
            log.info(String.format(Locale.ROOT, "The recommended layout %s doesn't meet minimum benefit ratio %f", best,
                    getMinBenefitRatio()));
            return false;
        }
        return true;
    }

    public double getMinBenefitRatio() {
        return layoutStats.getBpusMinBenefitRatio();
    }

    @Override
    public void propagateAggregationCost(BigInteger layout, Set<BigInteger> selected) {
        propagateAggregationCost(layout, selected, processLayoutAggCostMap);
    }

    private void propagateAggregationCost(BigInteger layout, Set<BigInteger> selected,
            Map<BigInteger, Long> processLayoutAggCostMap) {
        long aggregationCost = getLayoutCost(layout);
        Set<BigInteger> childrenLayouts = layoutStats.getAllDescendants(layout);
        for (BigInteger child : childrenLayouts) {
            if (!selected.contains(child) && (aggregationCost < getLayoutAggregationCost(child))) {
                processLayoutAggCostMap.put(child, aggregationCost);
            }
        }
    }

    /**
     * Return the space cost of building a layout.
     *
     */
    public double calculateSpaceCost(BigInteger layout) {
        return layoutStats.getLayoutCount(layout);
    }

    @Override
    public BenefitPolicy getInstance() {
        return new BPUSCalculator(this.layoutStats, this.initLayoutAggCostMap);
    }
}
