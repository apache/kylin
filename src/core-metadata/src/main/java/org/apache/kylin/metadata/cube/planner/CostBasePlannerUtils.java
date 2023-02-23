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

package org.apache.kylin.metadata.cube.planner;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.cube.cuboid.NAggregationGroup;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.cube.model.RuleBasedIndex;
import org.apache.kylin.metadata.cube.planner.algorithm.BenefitPolicy;
import org.apache.kylin.metadata.cube.planner.algorithm.LayoutRecommendAlgorithm;
import org.apache.kylin.metadata.cube.planner.algorithm.LayoutStats;
import org.apache.kylin.metadata.cube.planner.algorithm.PBPUSCalculator;
import org.apache.kylin.metadata.cube.planner.algorithm.genetic.GeneticAlgorithm;
import org.apache.kylin.metadata.cube.planner.algorithm.greedy.GreedyAlgorithm;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

public class CostBasePlannerUtils {
    private static final Logger logger = LoggerFactory.getLogger(CostBasePlannerUtils.class);

    public static Map<BigInteger, Long> getRecommendLayoutList(RuleBasedIndex ruleBasedIndex, KylinConfig kylinConf,
            String modelName, Map<BigInteger, Long> layoutRowCountMap, Map<BigInteger, Double> layoutSizeMap) {
        BigInteger baseLayout = generateBaseLayoutId(ruleBasedIndex);
        Set<BigInteger> mandatoryLayouts = generateMandatoryLayouts(ruleBasedIndex);
        logger.info("Build layout stats model name {}, baseLayout {}, mandatory layout {}, statistic layout {}",
                modelName, baseLayout, mandatoryLayouts, layoutRowCountMap.keySet());
        LayoutStats layoutStats = new LayoutStats.Builder(modelName, baseLayout, baseLayout, layoutRowCountMap,
                layoutSizeMap).setMandatoryLayouts(mandatoryLayouts)
                        .setBPUSMinBenefitRatio(kylinConf.getCostBasedPlannerBPUSMinBenefitRatio()).build();
        Map<BigInteger, Long> result = getRecommendLayoutList(layoutStats, kylinConf, !mandatoryLayouts.isEmpty());
        // if not recommend any layout and just apply all layouts with the rule base index
        if (result == null || result.isEmpty()) {
            result = new HashMap<>();
            Set<LayoutEntity> allLayouts = ruleBasedIndex.genCuboidLayouts();
            for (LayoutEntity layoutEntity : allLayouts) {
                BigInteger layout = convertDimensionsToLayoutId(layoutEntity.getDimsIds(),
                        ruleBasedIndex.countOfIncludeDimension(), ruleBasedIndex.getColumnIdToRowKeyId());
                result.put(layout, 0L);
            }
            logger.info("Not recommend any layout with the cost based method, and use the rule based layout {}",
                    result.keySet());
        }
        return result;
    }

    private static BigInteger generateBaseLayoutId(RuleBasedIndex ruleBasedIndex) {
        int dimensionCount = ruleBasedIndex.countOfIncludeDimension();
        List<Integer> dimensionIds = new ArrayList<>(ruleBasedIndex.getDimensions());
        return convertDimensionsToLayoutId(dimensionIds, dimensionCount, ruleBasedIndex.getColumnIdToRowKeyId());
    }

    private static Set<BigInteger> generateMandatoryLayouts(RuleBasedIndex ruleBasedIndex) {
        Set<BigInteger> result = new HashSet<>();
        if (ruleBasedIndex != null) {
            int dimensionCount = ruleBasedIndex.countOfIncludeDimension();
            for (NAggregationGroup aggregationGroup : ruleBasedIndex.getAggregationGroups()) {
                Integer[] mandatoryDimensionIds = aggregationGroup.getSelectRule().getMandatoryDims();
                // If there is no mandatory for the agg group, should not add the layout
                if (mandatoryDimensionIds != null && mandatoryDimensionIds.length != 0) {
                    BigInteger layoutId = convertDimensionsToLayoutId(mandatoryDimensionIds, dimensionCount,
                            ruleBasedIndex.getColumnIdToRowKeyId());
                    result.add(layoutId);
                }
            }
        }
        return result;
    }

    private static Map<BigInteger, Long> getRecommendLayoutList(LayoutStats layoutStats, KylinConfig kylinConf,
            boolean ifForceRecommend) {
        // default is 1<<7 = 128
        long threshold1 = 1L << kylinConf.getCostBasedPlannerGreedyAlgorithmAutoThreshold() - 1;
        // default is 1<<22 = 1024*1024*4
        long threshold2 = 1L << kylinConf.getCostBasedPlannerGeneticAlgorithmAutoThreshold() - 1;
        if (threshold1 >= threshold2) {
            logger.error("Invalid Cube Planner Algorithm configuration");
            return null;
        }
        int allLayoutCount = layoutStats.getAllLayoutsForMandatory().size()
                + layoutStats.getAllLayoutsForSelection().size();

        if (!ifForceRecommend && allLayoutCount < threshold1) {
            logger.info("Not recommend layout, the all layout count is {}, the threshold1 is {}, force recommend {}",
                    allLayoutCount, threshold1, ifForceRecommend);
            return null;
        }

        BenefitPolicy benefitPolicy = new PBPUSCalculator(layoutStats);
        LayoutRecommendAlgorithm algorithm;

        if (allLayoutCount <= threshold2) {
            algorithm = new GreedyAlgorithm(-1, benefitPolicy, layoutStats);
        } else {
            algorithm = new GeneticAlgorithm(-1, benefitPolicy, layoutStats);
        }
        long startTime = System.currentTimeMillis();
        logger.info("Cube Planner Algorithm started at {}", startTime);
        List<BigInteger> recommendLayoutList = algorithm
                .recommend(kylinConf.getCostBasedPlannerExpansionRateThreshold());
        logger.info("Cube Planner Algorithm ended at {}, cost time {}", System.currentTimeMillis(),
                System.currentTimeMillis() - startTime);

        if (recommendLayoutList.size() < allLayoutCount) {
            logger.info("Cube Planner Algorithm chooses {} most effective layouts to build among of all {} layouts.",
                    recommendLayoutList.size(), allLayoutCount);
        }
        Map<BigInteger, Long> recommendLayoutsWithStats = Maps.newLinkedHashMap();
        for (BigInteger layout : recommendLayoutList) {
            if (layout.equals(BigInteger.ZERO)) {
                // for zero layout, just simply recommend the cheapest layout.
                handleLayoutZeroRecommend(layoutStats, recommendLayoutsWithStats);
            } else {
                recommendLayoutsWithStats.put(layout, layoutStats.getLayoutCount(layout));
            }
        }
        return recommendLayoutsWithStats;
    }

    private static void handleLayoutZeroRecommend(LayoutStats layoutStats,
            Map<BigInteger, Long> recommendLayoutsWithStats) {
        Map<BigInteger, Long> statistics = layoutStats.getStatistics();
        BigInteger cheapestLayout = null;
        Long cheapestLayoutCount = Long.MAX_VALUE;
        for (Map.Entry<BigInteger, Long> layoutStatsEntry : statistics.entrySet()) {
            if (layoutStatsEntry.getValue() < cheapestLayoutCount) {
                cheapestLayout = layoutStatsEntry.getKey();
                cheapestLayoutCount = layoutStatsEntry.getValue();
            }
        }
        if (cheapestLayout != null) {
            logger.info("recommend layout:{} instead of layout zero", cheapestLayout);
            recommendLayoutsWithStats.put(cheapestLayout, cheapestLayoutCount);
        }
    }

    /**
     * convert the dimensionId list to layout
     */
    private static BigInteger convertDimensionsToLayoutId(Integer[] dimensionIds, int dimensionCount,
            Map<Integer, Integer> columnIdToRowkeyId) {
        Preconditions.checkNotNull(dimensionIds);
        Preconditions.checkArgument(dimensionIds.length != 0, "The length of dimensionIds must be greater than 0");
        BigInteger layout = BigInteger.ZERO;
        // If the dimension count is 12, the ids is [4,8,11]
        // and the result is 00000000,00000000,00000000,10001001
        for (Integer dimensionId : dimensionIds) {
            if (!columnIdToRowkeyId.containsKey(dimensionId)) {
                throw new RuntimeException("Can't find the rowkey id for the dimension id");
            }
            int rowkeyId = columnIdToRowkeyId.get(dimensionId);
            if (rowkeyId >= dimensionCount) {
                throw new RuntimeException("The rowkey id must less than the count of dimension");
            }
            // set `dimensionCount - 1 - rowkeyId`th bit to 1
            layout = layout.setBit(dimensionCount - 1 - rowkeyId);
        }
        return layout;
    }

    public static BigInteger convertDimensionsToLayoutId(List<Integer> dimensionIds, int dimensionCount,
            Map<Integer, Integer> columnIdToRowkeyId) {
        return convertDimensionsToLayoutId(dimensionIds.toArray(new Integer[0]), dimensionCount, columnIdToRowkeyId);
    }
}
