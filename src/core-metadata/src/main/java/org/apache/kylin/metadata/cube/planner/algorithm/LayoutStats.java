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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class LayoutStats {

    static final double WEIGHT_FOR_UN_QUERY = 0.2;
    static final double BPUS_MIN_BENEFIT_RATIO = 0.001;

    public static class Builder {

        private static final long THRESHOLD_ROLL_UP_FOR_MANDATORY = 1000L;

        // Required parameters
        private final String key;
        private final BigInteger nTotalLayouts;
        private final BigInteger baseLayout;
        private double queryUncertaintyRatio = WEIGHT_FOR_UN_QUERY;
        private double bpusMinBenefitRatio = BPUS_MIN_BENEFIT_RATIO;
        private Map<BigInteger, Long> statistics;
        private Map<BigInteger, Double> size;

        // Optional parameters - initialized to default values
        private Set<BigInteger> mandatoryLayouts = null;
        //// These two properties are for generating mandatory layouts
        private Map<BigInteger, Map<BigInteger, Pair<Long, Long>>> rollingUpCountSourceMap = null;

        private Map<BigInteger, Long> hitFrequencyMap = null;
        private Map<BigInteger, Map<BigInteger, Long>> scanCountSourceMap = null;

        public Builder(String key, BigInteger nTotalLayouts, BigInteger baseLayout, Map<BigInteger, Long> statistics,
                Map<BigInteger, Double> size) {
            this.key = key;
            this.nTotalLayouts = nTotalLayouts;
            this.baseLayout = baseLayout;
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

        public Builder setMandatoryLayouts(Set<BigInteger> mandatoryLayouts) {
            this.mandatoryLayouts = mandatoryLayouts;
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

        public Map<BigInteger, Double> estimateLayoutsSize(Map<BigInteger, Long> statistics) {
            return null;
        }

        public LayoutStats build() {
            Preconditions.checkNotNull(key, "key should not be null");
            Preconditions.checkNotNull(baseLayout, "baseLayout should not be null");
            Preconditions.checkNotNull(statistics, "statistics should not be null");
            Preconditions.checkNotNull(size, "size should not be null");
            Preconditions.checkNotNull(statistics.get(baseLayout),
                    "row count should exist for base layout " + baseLayout);
            Preconditions.checkState(statistics.keySet().equals(size.keySet()),
                    "statistics & size should own the same key set");
            statistics = LayoutStatsUtil.adjustLayoutStats(statistics);

            if (hitFrequencyMap != null && rollingUpCountSourceMap != null) {
                Map<BigInteger, Double> layoutHitProbabilityMap = LayoutStatsUtil.calculateLayoutHitProbability(
                        hitFrequencyMap.keySet(), hitFrequencyMap, nTotalLayouts, queryUncertaintyRatio);
                Map<BigInteger, Long> srcLayoutsStats = LayoutStatsUtil.generateSourceLayoutStats(statistics,
                        layoutHitProbabilityMap, rollingUpCountSourceMap);

                statistics.putAll(srcLayoutsStats);

                Map<BigInteger, Double> estimatedSize = estimateLayoutsSize(statistics);
                if (estimatedSize != null && !estimatedSize.isEmpty()) {
                    size = Maps.newHashMap(estimatedSize);
                }
            }

            if (mandatoryLayouts == null) {
                mandatoryLayouts = Sets.newHashSet();
            } else if (!mandatoryLayouts.isEmpty()) {
                statistics.putAll(LayoutStatsUtil.complementRowCountForLayouts(statistics, mandatoryLayouts));
            }

            return new LayoutStats(key, baseLayout, queryUncertaintyRatio, bpusMinBenefitRatio, mandatoryLayouts,
                    statistics, size, hitFrequencyMap, scanCountSourceMap);
        }
    }

    private final String key;
    private final BigInteger baseLayout;
    private final double bpusMinBenefitRatio;
    private final ImmutableSet<BigInteger> mandatoryLayoutSet;
    private final ImmutableSet<BigInteger> selectionLayoutSet;
    private final ImmutableMap<BigInteger, Long> layoutCountMap;
    private final ImmutableMap<BigInteger, Double> layoutSizeMap;
    private final ImmutableMap<BigInteger, Double> layoutHitProbabilityMap;
    private final ImmutableMap<BigInteger, Long> layoutScanCountMap;

    private final ImmutableMap<BigInteger, List<BigInteger>> directChildrenCache;
    private final Map<BigInteger, Set<BigInteger>> allDescendantsCache;

    private LayoutStats(String key, BigInteger baseLayoutId, double queryUncertaintyRatio, double bpusMinBenefitRatio,
            Set<BigInteger> mandatoryLayouts, Map<BigInteger, Long> statistics, Map<BigInteger, Double> size,
            Map<BigInteger, Long> hitFrequencyMap, Map<BigInteger, Map<BigInteger, Long>> scanCountSourceMap) {

        this.key = key;
        this.baseLayout = baseLayoutId;
        this.bpusMinBenefitRatio = bpusMinBenefitRatio;
        /* Initial mandatory layouts */
        Set<BigInteger> layoutsForMandatory = Sets.newHashSet(mandatoryLayouts);
        //Always add base layout.
        layoutsForMandatory.add(baseLayout);

        log.info("Mandatory layouts: " + layoutsForMandatory);

        /* Initial selection layouts */
        Set<BigInteger> layoutsForSelection = Sets.newHashSet(statistics.keySet());
        layoutsForSelection.removeAll(layoutsForMandatory);

        //There's no overlap between mandatoryLayoutSet and selectionLayoutSet
        this.mandatoryLayoutSet = ImmutableSet.<BigInteger> builder().addAll(layoutsForMandatory).build();
        this.selectionLayoutSet = ImmutableSet.<BigInteger> builder().addAll(layoutsForSelection).build();
        if (selectionLayoutSet.isEmpty()) {
            log.warn("The selection set should not be empty!!!");
        }

        this.layoutCountMap = ImmutableMap.<BigInteger, Long> builder().putAll(statistics).build();
        this.layoutSizeMap = ImmutableMap.<BigInteger, Double> builder().putAll(size).build();

        /* Initialize the hit probability for each selection layout */
        Map<BigInteger, Double> tmpLayoutHitProbabilityMap = LayoutStatsUtil.calculateLayoutHitProbability(
                selectionLayoutSet, hitFrequencyMap, BigInteger.valueOf(selectionLayoutSet.size()),
                queryUncertaintyRatio);
        this.layoutHitProbabilityMap = ImmutableMap.<BigInteger, Double> builder().putAll(tmpLayoutHitProbabilityMap)
                .build();

        /* Initialize the scan count when query for each selection layout + one base layout */
        Map<BigInteger, Long> tmpLayoutScanCountMap = Maps.newHashMapWithExpectedSize(1 + selectionLayoutSet.size());
        tmpLayoutScanCountMap.put(baseLayout, getExpScanCount(baseLayout, statistics, scanCountSourceMap));
        for (BigInteger layout : selectionLayoutSet) {
            tmpLayoutScanCountMap.put(layout, getExpScanCount(layout, statistics, scanCountSourceMap));
        }
        this.layoutScanCountMap = ImmutableMap.<BigInteger, Long> builder().putAll(tmpLayoutScanCountMap).build();

        this.directChildrenCache = ImmutableMap.<BigInteger, List<BigInteger>> builder()
                .putAll(LayoutStatsUtil.createDirectChildrenCache(statistics.keySet())).build();

        this.allDescendantsCache = Maps.newConcurrentMap();
    }

    private long getExpScanCount(BigInteger sourceLayout, Map<BigInteger, Long> statistics,
            Map<BigInteger, Map<BigInteger, Long>> scanCountSourceMap) {
        Preconditions.checkNotNull(statistics.get(sourceLayout),
                "The statistics for source layout " + sourceLayout + " does not exist!!!");
        if (scanCountSourceMap == null || scanCountSourceMap.get(sourceLayout) == null
                || scanCountSourceMap.get(sourceLayout).size() <= 0) {
            return statistics.get(sourceLayout);
        } else {
            Map<BigInteger, Long> scanCountTargetMap = scanCountSourceMap.get(sourceLayout);
            long totalEstScanCount = 0L;
            for (Map.Entry<BigInteger, Long> subEntry : scanCountTargetMap.entrySet()) {
                BigInteger targetLayout = subEntry.getKey();
                Preconditions.checkNotNull(statistics.get(targetLayout),
                        "The statistics for target layout %s does not exist!!!", targetLayout);
                // Consider the ratio of row count between source layout and target layout
                totalEstScanCount += subEntry.getValue() * statistics.get(sourceLayout) / statistics.get(targetLayout);
            }
            return totalEstScanCount / scanCountTargetMap.size();
        }
    }

    public double getBpusMinBenefitRatio() {
        return bpusMinBenefitRatio;
    }

    public Set<BigInteger> getAllDescendants(BigInteger layout) {
        Set<BigInteger> allDescendants = Sets.newLinkedHashSet();
        if (selectionLayoutSet.contains(layout)) {
            if (allDescendantsCache.get(layout) != null) {
                return allDescendantsCache.get(layout);
            } else {
                getAllDescendants(layout, allDescendants);
                allDescendantsCache.put(layout, allDescendants);
            }
        }
        return allDescendants;
    }

    private void getAllDescendants(BigInteger layout, Set<BigInteger> allDescendants) {
        if (allDescendants.contains(layout)) {
            return;
        }
        allDescendants.add(layout);
        for (BigInteger directChild : directChildrenCache.get(layout)) {
            getAllDescendants(directChild, allDescendants);
        }
    }

    public ImmutableSet<BigInteger> getAllLayoutsForSelection() {
        return selectionLayoutSet;
    }

    public ImmutableSet<BigInteger> getAllLayoutsForMandatory() {
        return mandatoryLayoutSet;
    }

    public Long getLayoutQueryCost(BigInteger layout) {
        return layoutScanCountMap.get(layout);
    }

    public Long getLayoutCount(BigInteger layout) {
        return layoutCountMap.get(layout);
    }

    public Double getLayoutSize(BigInteger layout) {
        return layoutSizeMap.get(layout);
    }

    public double getLayoutHitProb(BigInteger layout) {
        if (mandatoryLayoutSet.contains(layout)) {
            return 1;
        } else {
            return layoutHitProbabilityMap.get(layout) == null ? 0 : layoutHitProbabilityMap.get(layout);
        }
    }

    public Map<BigInteger, Long> getStatistics() {
        return layoutCountMap;
    }

    public double getBaseLayoutSize() {
        return getLayoutSize(baseLayout);
    }

    public BigInteger getBaseLayout() {
        return baseLayout;
    }

    public String getKey() {
        return key;
    }

    public LayoutBenefitModel.LayoutModel getLayoutModel(BigInteger layout) {
        return new LayoutBenefitModel.LayoutModel(layout, getLayoutCount(layout), getLayoutSize(layout),
                getLayoutHitProb(layout), getLayoutQueryCost(layout));
    }
}
