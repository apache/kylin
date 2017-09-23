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

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class CuboidStats {
    private static final Logger logger = LoggerFactory.getLogger(CuboidStats.class);

    public static class Builder {

        private static final long THRESHOLD_ROLL_UP_FOR_MANDATORY = 1000L;

        // Required parameters
        private String key;
        private Long baseCuboid;
        private Map<Long, Long> statistics;
        private Map<Long, Double> size;

        // Optional parameters - initialized to default values
        private Set<Long> mandatoryCuboids = null;
        //// These two properties are for generating mandatory cuboids
        private Map<Long, Map<Long, Long>> rollingUpCountSourceMap = null;
        private Long rollUpThresholdForMandatory = null;

        private Map<Long, Long> hitFrequencyMap = null;
        private Map<Long, Map<Long, Long>> scanCountSourceMap = null;

        public Builder(String key, Long baseCuboid, Map<Long, Long> statistics, Map<Long, Double> size) {
            this.key = key;
            this.baseCuboid = baseCuboid;
            this.statistics = statistics;
            this.size = size;
        }

        public Builder setRollingUpCountSourceMap(Map<Long, Map<Long, Long>> rollingUpCountSourceMap) {
            this.rollingUpCountSourceMap = rollingUpCountSourceMap;
            this.rollUpThresholdForMandatory = THRESHOLD_ROLL_UP_FOR_MANDATORY;
            return this;
        }

        public Builder setRollingUpCountSourceMap(Map<Long, Map<Long, Long>> rollingUpCountSourceMap,
                long rollUpThresholdForMandatory) {
            this.rollingUpCountSourceMap = rollingUpCountSourceMap;
            this.rollUpThresholdForMandatory = rollUpThresholdForMandatory;
            return this;
        }

        public Builder setMandatoryCuboids(Set<Long> mandatoryCuboids) {
            this.mandatoryCuboids = mandatoryCuboids;
            return this;
        }

        public Builder setHitFrequencyMap(Map<Long, Long> hitFrequencyMap) {
            this.hitFrequencyMap = hitFrequencyMap;
            return this;
        }

        public Builder setScanCountSourceMap(Map<Long, Map<Long, Long>> scanCountSourceMap) {
            this.scanCountSourceMap = scanCountSourceMap;
            return this;
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
            if (mandatoryCuboids == null) {
                mandatoryCuboids = Sets.newHashSet();
            }
            if (rollingUpCountSourceMap != null) {
                mandatoryCuboids.addAll(CuboidStatsUtil.generateMandatoryCuboidSet(statistics, hitFrequencyMap,
                        rollingUpCountSourceMap, rollUpThresholdForMandatory));
            }

            return new CuboidStats(key, baseCuboid, mandatoryCuboids, statistics, size, hitFrequencyMap,
                    scanCountSourceMap);
        }
    }

    private static final double WEIGHT_FOR_UN_QUERY = 0.2;

    private String key;
    private long baseCuboid;
    private ImmutableSet<Long> mandatoryCuboidSet;
    private ImmutableSet<Long> selectionCuboidSet;
    private ImmutableMap<Long, Long> cuboidCountMap;
    private ImmutableMap<Long, Double> cuboidSizeMap;
    private ImmutableMap<Long, Double> cuboidHitProbabilityMap;
    private ImmutableMap<Long, Long> cuboidScanCountMap;

    private ImmutableMap<Long, List<Long>> directChildrenCache;
    private Map<Long, Set<Long>> allDescendantsCache;

    private CuboidStats(String key, long baseCuboidId, Set<Long> mandatoryCuboids, Map<Long, Long> statistics,
            Map<Long, Double> size, Map<Long, Long> hitFrequencyMap, Map<Long, Map<Long, Long>> scanCountSourceMap) {

        this.key = key;
        this.baseCuboid = baseCuboidId;
        /** Initial mandatory cuboids */
        Set<Long> cuboidsForMandatory = Sets.newHashSet(mandatoryCuboids);
        //Always add base cuboid.
        if (!cuboidsForMandatory.contains(baseCuboid)) {
            cuboidsForMandatory.add(baseCuboid);
        }
        logger.info("Mandatory cuboids: " + cuboidsForMandatory);

        /** Initial selection cuboids */
        Set<Long> cuboidsForSelection = Sets.newHashSet(statistics.keySet());
        cuboidsForSelection.removeAll(cuboidsForMandatory);

        //There's no overlap between mandatoryCuboidSet and selectionCuboidSet
        this.mandatoryCuboidSet = ImmutableSet.<Long> builder().addAll(cuboidsForMandatory).build();
        this.selectionCuboidSet = ImmutableSet.<Long> builder().addAll(cuboidsForSelection).build();
        if (selectionCuboidSet.isEmpty()) {
            logger.warn("The selection set should not be empty!!!");
        }

        /** Initialize row count for mandatory cuboids */
        CuboidStatsUtil.complementRowCountForMandatoryCuboids(statistics, baseCuboid, mandatoryCuboidSet);

        this.cuboidCountMap = ImmutableMap.<Long, Long> builder().putAll(statistics).build();
        this.cuboidSizeMap = ImmutableMap.<Long, Double> builder().putAll(size).build();

        /** Initialize the hit probability for each selection cuboid */
        Map<Long, Double> tmpCuboidHitProbabilityMap = Maps.newHashMapWithExpectedSize(selectionCuboidSet.size());
        if (hitFrequencyMap != null) {
            long totalHitFrequency = 0L;
            for (Map.Entry<Long, Long> hitFrequency : hitFrequencyMap.entrySet()) {
                if (selectionCuboidSet.contains(hitFrequency.getKey())) {
                    totalHitFrequency += hitFrequency.getValue();
                }
            }

            final double unitUncertainProb = WEIGHT_FOR_UN_QUERY / selectionCuboidSet.size();
            for (Long cuboid : selectionCuboidSet) {
                //Calculate hit probability for each cuboid
                if (hitFrequencyMap.get(cuboid) != null) {
                    tmpCuboidHitProbabilityMap.put(cuboid, unitUncertainProb
                            + (1 - WEIGHT_FOR_UN_QUERY) * hitFrequencyMap.get(cuboid) / totalHitFrequency);
                } else {
                    tmpCuboidHitProbabilityMap.put(cuboid, unitUncertainProb);
                }
            }
        } else {
            for (Long cuboid : selectionCuboidSet) {
                tmpCuboidHitProbabilityMap.put(cuboid, 1.0 / selectionCuboidSet.size());
            }
        }
        this.cuboidHitProbabilityMap = ImmutableMap.<Long, Double> builder().putAll(tmpCuboidHitProbabilityMap).build();

        /** Initialize the scan count when query for each selection cuboid + one base cuboid */
        Map<Long, Long> tmpCuboidScanCountMap = Maps.newHashMapWithExpectedSize(1 + selectionCuboidSet.size());
        tmpCuboidScanCountMap.put(baseCuboid, getExpScanCount(baseCuboid, statistics, scanCountSourceMap));
        for (Long cuboid : selectionCuboidSet) {
            tmpCuboidScanCountMap.put(cuboid, getExpScanCount(cuboid, statistics, scanCountSourceMap));
        }
        this.cuboidScanCountMap = ImmutableMap.<Long, Long> builder().putAll(tmpCuboidScanCountMap).build();

        this.directChildrenCache = ImmutableMap.<Long, List<Long>> builder()
                .putAll(CuboidStatsUtil.createDirectChildrenCache(statistics.keySet())).build();

        this.allDescendantsCache = Maps.newConcurrentMap();
    }

    private long getExpScanCount(long sourceCuboid, Map<Long, Long> statistics,
            Map<Long, Map<Long, Long>> scanCountSourceMap) {
        Preconditions.checkNotNull(statistics.get(sourceCuboid),
                "The statistics for source cuboid " + sourceCuboid + " does not exist!!!");
        if (scanCountSourceMap == null || scanCountSourceMap.get(sourceCuboid) == null
                || scanCountSourceMap.get(sourceCuboid).size() <= 0) {
            return statistics.get(sourceCuboid);
        } else {
            //TODO some improvement can be done by assigning weights based on distance between source cuboid and target cuboid
            Map<Long, Long> scanCountTargetMap = scanCountSourceMap.get(sourceCuboid);
            long totalEstScanCount = 0L;
            for (Map.Entry<Long, Long> subEntry : scanCountTargetMap.entrySet()) {
                long targetCuboid = subEntry.getKey();
                Preconditions.checkNotNull(statistics.get(targetCuboid),
                        "The statistics for target cuboid " + targetCuboid + " does not exist!!!");
                // Consider the ratio of row count between source cuboid and target cuboid
                totalEstScanCount += subEntry.getValue() * statistics.get(sourceCuboid) / statistics.get(targetCuboid);
            }
            return totalEstScanCount / scanCountTargetMap.size();
        }
    }

    public Set<Long> getAllDescendants(long cuboid) {
        Set<Long> allDescendants = Sets.newLinkedHashSet();
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

    private void getAllDescendants(long cuboid, Set<Long> allDescendants) {
        if (allDescendants.contains(cuboid)) {
            return;
        }
        allDescendants.add(cuboid);
        for (Long directChild : directChildrenCache.get(cuboid)) {
            getAllDescendants(directChild, allDescendants);
        }
    }

    public Set<Long> getAllCuboidsForSelection() {
        return selectionCuboidSet;
    }

    public Set<Long> getAllCuboidsForMandatory() {
        return mandatoryCuboidSet;
    }

    public Long getCuboidQueryCost(long cuboid) {
        return cuboidScanCountMap.get(cuboid);
    }

    public Long getCuboidCount(long cuboid) {
        return cuboidCountMap.get(cuboid);
    }

    public Double getCuboidSize(long cuboid) {
        return cuboidSizeMap.get(cuboid);
    }

    public double getCuboidHitProbability(long cuboid) {
        if (mandatoryCuboidSet.contains(cuboid)) {
            return 1;
        } else {
            return cuboidHitProbabilityMap.get(cuboid) == null ? 0 : cuboidHitProbabilityMap.get(cuboid);
        }
    }

    public Map<Long, Long> getStatistics() {
        return cuboidCountMap;
    }

    public double getBaseCuboidSize() {
        return getCuboidSize(baseCuboid);
    }

    public long getBaseCuboid() {
        return baseCuboid;
    }

    public String getKey() {
        return key;
    }

    public CuboidBenefitModel.CuboidModel getCuboidModel(long cuboid) {
        return new CuboidBenefitModel.CuboidModel(cuboid, getCuboidCount(cuboid), getCuboidSize(cuboid),
                getCuboidHitProbability(cuboid), getCuboidQueryCost(cuboid));
    }
}
