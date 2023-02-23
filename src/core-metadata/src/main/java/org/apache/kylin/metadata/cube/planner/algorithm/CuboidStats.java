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
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kylin.common.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class CuboidStats {
    private static final Logger logger = LoggerFactory.getLogger(CuboidStats.class);

    static final double WEIGHT_FOR_UN_QUERY = 0.2;
    static final double BPUS_MIN_BENEFIT_RATIO = 0.001;

    public static class Builder {

        private static final long THRESHOLD_ROLL_UP_FOR_MANDATORY = 1000L;

        // Required parameters
        private String key;
        private BigInteger nTotalCuboids;
        private BigInteger baseCuboid;
        private double queryUncertaintyRatio = WEIGHT_FOR_UN_QUERY;
        private double bpusMinBenefitRatio = BPUS_MIN_BENEFIT_RATIO;
        private Map<BigInteger, Long> statistics;
        private Map<BigInteger, Double> size;

        // Optional parameters - initialized to default values
        private Set<BigInteger> mandatoryCuboids = null;
        //// These two properties are for generating mandatory cuboids
        private Map<BigInteger, Map<BigInteger, Pair<Long, Long>>> rollingUpCountSourceMap = null;

        private Map<BigInteger, Long> hitFrequencyMap = null;
        private Map<BigInteger, Map<BigInteger, Long>> scanCountSourceMap = null;

        public Builder(String key, BigInteger nTotalCuboids, BigInteger baseCuboid, Map<BigInteger, Long> statistics,
                Map<BigInteger, Double> size) {
            this.key = key;
            this.nTotalCuboids = nTotalCuboids;
            this.baseCuboid = baseCuboid;
            this.statistics = statistics;
            this.size = size;
        }

        public Builder setQueryUncertaintyRatio(double queryUncertaintyRatio) {
            this.queryUncertaintyRatio = queryUncertaintyRatio;
            return this;
        }

        public Builder setBPUSMinBenefitRatio(double bpusMinBenefitRatio) {
            this.bpusMinBenefitRatio = bpusMinBenefitRatio;
            return this;
        }

        public Builder setRollingUpCountSourceMap(
                Map<BigInteger, Map<BigInteger, Pair<Long, Long>>> rollingUpCountSourceMap) {
            this.rollingUpCountSourceMap = rollingUpCountSourceMap;
            return this;
        }

        public Builder setMandatoryCuboids(Set<BigInteger> mandatoryCuboids) {
            this.mandatoryCuboids = mandatoryCuboids;
            return this;
        }

        public Builder setHitFrequencyMap(Map<BigInteger, Long> hitFrequencyMap) {
            this.hitFrequencyMap = hitFrequencyMap;
            return this;
        }

        public Builder setScanCountSourceMap(Map<BigInteger, Map<BigInteger, Long>> scanCountSourceMap) {
            this.scanCountSourceMap = scanCountSourceMap;
            return this;
        }

        public Map<BigInteger, Double> estimateCuboidsSize(Map<BigInteger, Long> statistics) {
            return null;
        }

        public CuboidStats build() {
            Preconditions.checkNotNull(key, "key should not be null");
            Preconditions.checkNotNull(baseCuboid, "baseCuboid should not be null");
            Preconditions.checkNotNull(statistics, "statistics should not be null");
            Preconditions.checkNotNull(size, "size should not be null");
            Preconditions.checkNotNull(statistics.get(baseCuboid),
                    "row count should exist for base cuboid " + baseCuboid);
            Preconditions.checkState(statistics.keySet().equals(size.keySet()),
                    "statistics & size should own the same key set");
            statistics = CuboidStatsUtil.adjustCuboidStats(statistics);

            if (hitFrequencyMap != null && rollingUpCountSourceMap != null) {
                Map<BigInteger, Double> cuboidHitProbabilityMap = CuboidStatsUtil.calculateCuboidHitProbability(
                        hitFrequencyMap.keySet(), hitFrequencyMap, nTotalCuboids, queryUncertaintyRatio);
                Map<BigInteger, Long> srcCuboidsStats = CuboidStatsUtil.generateSourceCuboidStats(statistics,
                        cuboidHitProbabilityMap, rollingUpCountSourceMap);

                statistics.putAll(srcCuboidsStats);

                Map<BigInteger, Double> estimatedSize = estimateCuboidsSize(statistics);
                if (estimatedSize != null && !estimatedSize.isEmpty()) {
                    size = Maps.newHashMap(estimatedSize);
                }
            }

            if (mandatoryCuboids == null) {
                mandatoryCuboids = Sets.newHashSet();
            } else if (!mandatoryCuboids.isEmpty()) {
                statistics.putAll(CuboidStatsUtil.complementRowCountForCuboids(statistics, mandatoryCuboids));
            }

            return new CuboidStats(key, baseCuboid, queryUncertaintyRatio, bpusMinBenefitRatio, mandatoryCuboids,
                    statistics, size, hitFrequencyMap, scanCountSourceMap);
        }
    }

    private String key;
    private BigInteger baseCuboid;
    private double bpusMinBenefitRatio;
    private ImmutableSet<BigInteger> mandatoryCuboidSet;
    private ImmutableSet<BigInteger> selectionCuboidSet;
    private ImmutableMap<BigInteger, Long> cuboidCountMap;
    private ImmutableMap<BigInteger, Double> cuboidSizeMap;
    private ImmutableMap<BigInteger, Double> cuboidHitProbabilityMap;
    private ImmutableMap<BigInteger, Long> cuboidScanCountMap;

    private ImmutableMap<BigInteger, List<BigInteger>> directChildrenCache;
    private Map<BigInteger, Set<BigInteger>> allDescendantsCache;

    private CuboidStats(String key, BigInteger baseCuboidId, double queryUncertaintyRatio, double bpusMinBenefitRatio,
            Set<BigInteger> mandatoryCuboids, Map<BigInteger, Long> statistics, Map<BigInteger, Double> size,
            Map<BigInteger, Long> hitFrequencyMap, Map<BigInteger, Map<BigInteger, Long>> scanCountSourceMap) {

        this.key = key;
        this.baseCuboid = baseCuboidId;
        this.bpusMinBenefitRatio = bpusMinBenefitRatio;
        /** Initial mandatory cuboids */
        Set<BigInteger> cuboidsForMandatory = Sets.newHashSet(mandatoryCuboids);
        //Always add base cuboid.
        if (!cuboidsForMandatory.contains(baseCuboid)) {
            cuboidsForMandatory.add(baseCuboid);
        }
        logger.info("Mandatory cuboids: " + cuboidsForMandatory);

        /** Initial selection cuboids */
        Set<BigInteger> cuboidsForSelection = Sets.newHashSet(statistics.keySet());
        cuboidsForSelection.removeAll(cuboidsForMandatory);

        //There's no overlap between mandatoryCuboidSet and selectionCuboidSet
        this.mandatoryCuboidSet = ImmutableSet.<BigInteger> builder().addAll(cuboidsForMandatory).build();
        this.selectionCuboidSet = ImmutableSet.<BigInteger> builder().addAll(cuboidsForSelection).build();
        if (selectionCuboidSet.isEmpty()) {
            logger.warn("The selection set should not be empty!!!");
        }

        this.cuboidCountMap = ImmutableMap.<BigInteger, Long> builder().putAll(statistics).build();
        this.cuboidSizeMap = ImmutableMap.<BigInteger, Double> builder().putAll(size).build();

        /** Initialize the hit probability for each selection cuboid */
        Map<BigInteger, Double> tmpCuboidHitProbabilityMap = CuboidStatsUtil.calculateCuboidHitProbability(
                selectionCuboidSet, hitFrequencyMap, BigInteger.valueOf(selectionCuboidSet.size()),
                queryUncertaintyRatio);
        this.cuboidHitProbabilityMap = ImmutableMap.<BigInteger, Double> builder().putAll(tmpCuboidHitProbabilityMap)
                .build();

        /** Initialize the scan count when query for each selection cuboid + one base cuboid */
        Map<BigInteger, Long> tmpCuboidScanCountMap = Maps.newHashMapWithExpectedSize(1 + selectionCuboidSet.size());
        tmpCuboidScanCountMap.put(baseCuboid, getExpScanCount(baseCuboid, statistics, scanCountSourceMap));
        for (BigInteger cuboid : selectionCuboidSet) {
            tmpCuboidScanCountMap.put(cuboid, getExpScanCount(cuboid, statistics, scanCountSourceMap));
        }
        this.cuboidScanCountMap = ImmutableMap.<BigInteger, Long> builder().putAll(tmpCuboidScanCountMap).build();

        this.directChildrenCache = ImmutableMap.<BigInteger, List<BigInteger>> builder()
                .putAll(CuboidStatsUtil.createDirectChildrenCache(statistics.keySet())).build();

        this.allDescendantsCache = Maps.newConcurrentMap();
    }

    private long getExpScanCount(BigInteger sourceCuboid, Map<BigInteger, Long> statistics,
            Map<BigInteger, Map<BigInteger, Long>> scanCountSourceMap) {
        Preconditions.checkNotNull(statistics.get(sourceCuboid),
                "The statistics for source cuboid " + sourceCuboid + " does not exist!!!");
        if (scanCountSourceMap == null || scanCountSourceMap.get(sourceCuboid) == null
                || scanCountSourceMap.get(sourceCuboid).size() <= 0) {
            return statistics.get(sourceCuboid);
        } else {
            Map<BigInteger, Long> scanCountTargetMap = scanCountSourceMap.get(sourceCuboid);
            long totalEstScanCount = 0L;
            for (Map.Entry<BigInteger, Long> subEntry : scanCountTargetMap.entrySet()) {
                BigInteger targetCuboid = subEntry.getKey();
                Preconditions.checkNotNull(statistics.get(targetCuboid),
                        "The statistics for target cuboid " + targetCuboid + " does not exist!!!");
                // Consider the ratio of row count between source cuboid and target cuboid
                totalEstScanCount += subEntry.getValue() * statistics.get(sourceCuboid) / statistics.get(targetCuboid);
            }
            return totalEstScanCount / scanCountTargetMap.size();
        }
    }

    public double getBpusMinBenefitRatio() {
        return bpusMinBenefitRatio;
    }

    public Set<BigInteger> getAllDescendants(BigInteger cuboid) {
        Set<BigInteger> allDescendants = Sets.newLinkedHashSet();
        if (selectionCuboidSet.contains(cuboid)) {
            if (allDescendantsCache.get(cuboid) != null) {
                return allDescendantsCache.get(cuboid);
            } else {
                getAllDescendants(cuboid, allDescendants);
                allDescendantsCache.put(cuboid, allDescendants);
            }
        }
        return allDescendants;
    }

    private void getAllDescendants(BigInteger cuboid, Set<BigInteger> allDescendants) {
        if (allDescendants.contains(cuboid)) {
            return;
        }
        allDescendants.add(cuboid);
        for (BigInteger directChild : directChildrenCache.get(cuboid)) {
            getAllDescendants(directChild, allDescendants);
        }
    }

    public ImmutableSet<BigInteger> getAllCuboidsForSelection() {
        return selectionCuboidSet;
    }

    public ImmutableSet<BigInteger> getAllCuboidsForMandatory() {
        return mandatoryCuboidSet;
    }

    public Long getCuboidQueryCost(BigInteger cuboid) {
        return cuboidScanCountMap.get(cuboid);
    }

    public Long getCuboidCount(BigInteger cuboid) {
        return cuboidCountMap.get(cuboid);
    }

    public Double getCuboidSize(BigInteger cuboid) {
        return cuboidSizeMap.get(cuboid);
    }

    public double getCuboidHitProbability(BigInteger cuboid) {
        if (mandatoryCuboidSet.contains(cuboid)) {
            return 1;
        } else {
            return cuboidHitProbabilityMap.get(cuboid) == null ? 0 : cuboidHitProbabilityMap.get(cuboid);
        }
    }

    public Map<BigInteger, Long> getStatistics() {
        return cuboidCountMap;
    }

    public double getBaseCuboidSize() {
        return getCuboidSize(baseCuboid);
    }

    public BigInteger getBaseCuboid() {
        return baseCuboid;
    }

    public String getKey() {
        return key;
    }

    public CuboidBenefitModel.CuboidModel getCuboidModel(BigInteger cuboid) {
        return new CuboidBenefitModel.CuboidModel(cuboid, getCuboidCount(cuboid), getCuboidSize(cuboid),
                getCuboidHitProbability(cuboid), getCuboidQueryCost(cuboid));
    }
}
