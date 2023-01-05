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
import org.apache.kylin.metadata.cube.planner.algorithm.CuboidRecommendAlgorithm;
import org.apache.kylin.metadata.cube.planner.algorithm.CuboidStats;
import org.apache.kylin.metadata.cube.planner.algorithm.PBPUSCalculator;
import org.apache.kylin.metadata.cube.planner.algorithm.genetic.GeneticAlgorithm;
import org.apache.kylin.metadata.cube.planner.algorithm.greedy.GreedyAlgorithm;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

public class CostBasePlannerUtils {
    private static final Logger logger = LoggerFactory.getLogger(CostBasePlannerUtils.class);

    public static Map<BigInteger, Long> getRecommendCuboidList(RuleBasedIndex ruleBasedIndex, KylinConfig kylinConf,
            String modelName, Map<BigInteger, Long> cuboidRowCountMap, Map<BigInteger, Double> cuboidSizeMap) {
        BigInteger baseCuboid = generateBaseCuboId(ruleBasedIndex);
        Set<BigInteger> mandatoryCuboids = generateMandatoryCuboIds(ruleBasedIndex);
        logger.info("Build cuboid stats model name {}, baseCuboid {}, mandatory cuboid {}, statistic cuboid {}",
                modelName, baseCuboid, mandatoryCuboids, cuboidRowCountMap.keySet());
        CuboidStats cuboidStats = new CuboidStats.Builder(modelName, baseCuboid, baseCuboid, cuboidRowCountMap,
                cuboidSizeMap).setMandatoryCuboids(mandatoryCuboids)
                        .setBPUSMinBenefitRatio(kylinConf.getCostBasedPlannerBPUSMinBenefitRatio()).build();
        Map<BigInteger, Long> result = getRecommendCuboidList(cuboidStats, kylinConf, !mandatoryCuboids.isEmpty());
        // if not recommend any cuboid and just apply all layouts with the rule base index
        if (result == null || result.isEmpty()) {
            result = new HashMap<>();
            Set<LayoutEntity> allLayouts = ruleBasedIndex.genCuboidLayouts();
            for (LayoutEntity layoutEntity : allLayouts) {
                BigInteger cuboid = convertDimensionsToCuboId(layoutEntity.getDimsIds(),
                        ruleBasedIndex.countOfIncludeDimension(), ruleBasedIndex.getColumnIdToRowKeyId());
                result.put(cuboid, 0L);
            }
            logger.info("Not recommend any cuboid with the cost based method, and use the rule based cuboid {}",
                    result.keySet());
        }
        return result;
    }

    private static BigInteger generateBaseCuboId(RuleBasedIndex ruleBasedIndex) {
        int dimensionCount = ruleBasedIndex.countOfIncludeDimension();
        List<Integer> dimensionIds = new ArrayList<>(ruleBasedIndex.getDimensions());
        BigInteger cuboid = convertDimensionsToCuboId(dimensionIds, dimensionCount,
                ruleBasedIndex.getColumnIdToRowKeyId());
        return cuboid;
    }

    private static Set<BigInteger> generateMandatoryCuboIds(RuleBasedIndex ruleBasedIndex) {
        Set<BigInteger> result = new HashSet<>();
        if (ruleBasedIndex != null) {
            int dimensionCount = ruleBasedIndex.countOfIncludeDimension();
            for (NAggregationGroup aggregationGroup : ruleBasedIndex.getAggregationGroups()) {
                Integer[] mandatoryDimensionIds = aggregationGroup.getSelectRule().getMandatoryDims();
                // If there is no mandatory for the agg group, should not add the cuboid
                if (mandatoryDimensionIds != null && mandatoryDimensionIds.length != 0) {
                    BigInteger cuboid = convertDimensionsToCuboId(mandatoryDimensionIds, dimensionCount,
                            ruleBasedIndex.getColumnIdToRowKeyId());
                    result.add(cuboid);
                }
            }
        }
        return result;
    }

    private static Map<BigInteger, Long> getRecommendCuboidList(CuboidStats cuboidStats, KylinConfig kylinConf,
            boolean ifForceRecommend) {
        // default is 1<<7 = 128
        long threshold1 = 1L << kylinConf.getCostBasedPlannerGreedyAlgorithmAutoThreshold() - 1;
        // default is 1<<22 = 1024*1024*4
        long threshold2 = 1L << kylinConf.getCostBasedPlannerGeneticAlgorithmAutoThreshold() - 1;
        if (threshold1 >= threshold2) {
            logger.error("Invalid Cube Planner Algorithm configuration");
            return null;
        }
        int allCuboidCount = cuboidStats.getAllCuboidsForMandatory().size()
                + cuboidStats.getAllCuboidsForSelection().size();

        if (!ifForceRecommend && allCuboidCount < threshold1) {
            logger.info("Not recommend cuboid, the all cuboid count is {}, the threshold1 is {}, force recommend {}",
                    allCuboidCount, threshold1, ifForceRecommend);
            return null;
        }

        BenefitPolicy benefitPolicy = new PBPUSCalculator(cuboidStats);
        CuboidRecommendAlgorithm algorithm = null;

        if (allCuboidCount <= threshold2) {
            algorithm = new GreedyAlgorithm(-1, benefitPolicy, cuboidStats);
        } else {
            algorithm = new GeneticAlgorithm(-1, benefitPolicy, cuboidStats);
        }
        long startTime = System.currentTimeMillis();
        logger.info("Cube Planner Algorithm started at {}", startTime);
        List<BigInteger> recommendCuboidList = algorithm
                .recommend(kylinConf.getCostBasedPlannerExpansionRateThreshold());
        logger.info("Cube Planner Algorithm ended at {}, cost time {}", System.currentTimeMillis(),
                System.currentTimeMillis() - startTime);

        if (recommendCuboidList.size() < allCuboidCount) {
            logger.info("Cube Planner Algorithm chooses {} most effective cuboids to build among of all {} cuboids.",
                    recommendCuboidList.size(), allCuboidCount);
        }
        Map<BigInteger, Long> recommendCuboidsWithStats = Maps.newLinkedHashMap();
        for (BigInteger cuboid : recommendCuboidList) {
            if (cuboid.equals(BigInteger.ZERO)) {
                // for zero cuboid, just simply recommend the cheapest cuboid.
                handleCuboidZeroRecommend(cuboidStats, recommendCuboidsWithStats);
            } else {
                recommendCuboidsWithStats.put(cuboid, cuboidStats.getCuboidCount(cuboid));
            }
        }
        return recommendCuboidsWithStats;
    }

    private static void handleCuboidZeroRecommend(CuboidStats cuboidStats,
            Map<BigInteger, Long> recommendCuboidsWithStats) {
        Map<BigInteger, Long> statistics = cuboidStats.getStatistics();
        BigInteger cheapestCuboid = null;
        Long cheapestCuboidCount = Long.MAX_VALUE;
        for (Map.Entry<BigInteger, Long> cuboidStatsEntry : statistics.entrySet()) {
            if (cuboidStatsEntry.getValue() < cheapestCuboidCount) {
                cheapestCuboid = cuboidStatsEntry.getKey();
                cheapestCuboidCount = cuboidStatsEntry.getValue();
            }
        }
        if (cheapestCuboid != null) {
            logger.info("recommend cuboid:{} instead of cuboid zero", cheapestCuboid);
            recommendCuboidsWithStats.put(cheapestCuboid, cheapestCuboidCount);
        }
    }

    /**
     * convert the dimensionid list to cuboid
     *
     * @param dimensionIds
     * @param dimensionCount
     * @return
     */
    private static BigInteger convertDimensionsToCuboId(Integer[] dimensionIds, int dimensionCount,
            Map<Integer, Integer> columnIdToRowkeyId) {
        Preconditions.checkNotNull(dimensionIds);
        Preconditions.checkArgument(dimensionIds.length != 0, "The length of dimensionIds must be greater than 0");
        BigInteger cuboid = BigInteger.ZERO;
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
            cuboid = cuboid.setBit(dimensionCount - 1 - rowkeyId);
        }
        return cuboid;
    }

    public static BigInteger convertDimensionsToCuboId(List<Integer> dimensionIds, int dimensionCount,
            Map<Integer, Integer> columnIdToRowkeyId) {
        return convertDimensionsToCuboId(dimensionIds.toArray(new Integer[0]), dimensionCount, columnIdToRowkeyId);
    }
}
