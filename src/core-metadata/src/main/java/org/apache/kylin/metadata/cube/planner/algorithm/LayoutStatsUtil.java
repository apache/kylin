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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.kylin.common.util.Pair;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class LayoutStatsUtil {

    private LayoutStatsUtil() {
        throw new IllegalStateException("Class LayoutStatsUtil is an utility class !");
    }

    /**
     * According to the layout hit frequencies and query uncertainty ratio
     * calculate each layout hit probability
     * @param selectionLayoutSet subset of layout domain which needs probability
     * @param nTotalLayouts number of layouts needs to be considered, mainly for each layout's uncertainty weight
     * */
    public static Map<BigInteger, Double> calculateLayoutHitProbability(Set<BigInteger> selectionLayoutSet,
            Map<BigInteger, Long> hitFrequencyMap, BigInteger nTotalLayouts, double queryUncertaintyRatio) {
        Map<BigInteger, Double> layoutHitProbabilityMap = Maps.newHashMapWithExpectedSize(selectionLayoutSet.size());
        if (hitFrequencyMap == null || hitFrequencyMap.isEmpty()) {
            double value = BigDecimal.valueOf(1.0).divide(new BigDecimal(nTotalLayouts), 15, RoundingMode.HALF_EVEN)
                    .doubleValue();
            for (BigInteger layout : selectionLayoutSet) {
                layoutHitProbabilityMap.put(layout, value);
            }
        } else {
            long totalHitFrequency = 0L;
            for (Map.Entry<BigInteger, Long> hitFrequency : hitFrequencyMap.entrySet()) {
                totalHitFrequency += hitFrequency.getValue();
            }

            final double unitUncertainProb = BigDecimal.valueOf(queryUncertaintyRatio)
                    .divide(new BigDecimal(nTotalLayouts), 15, RoundingMode.HALF_EVEN).doubleValue();
            for (BigInteger layout : selectionLayoutSet) {
                //Calculate hit probability for each layout
                if (hitFrequencyMap.get(layout) != null) {
                    if (totalHitFrequency != 0)
                        layoutHitProbabilityMap.put(layout, unitUncertainProb
                                + (1 - queryUncertaintyRatio) * hitFrequencyMap.get(layout) / totalHitFrequency);
                    else
                        throw new ArithmeticException("/ by zero");
                } else {
                    layoutHitProbabilityMap.put(layout, unitUncertainProb);
                }
            }
        }

        return layoutHitProbabilityMap;
    }

    /**
     * @param statistics for layout row count
     * @param rollingUpSourceMap the key of the outer map is source layout,
     *                           the key of the inner map is target layout,
     *                                  if cube is optimized multiple times, target layout may change
     *                           the first element of the pair is the rollup row count
     *                           the second element of the pair is the return row count
     * @return source layouts with estimated row count
     */
    public static Map<BigInteger, Long> generateSourceLayoutStats(Map<BigInteger, Long> statistics,
            Map<BigInteger, Double> layoutHitProbMap,
            Map<BigInteger, Map<BigInteger, Pair<Long, Long>>> rollingUpSourceMap) {
        Map<BigInteger, Long> srcLayoutsStats = Maps.newHashMap();
        if (layoutHitProbMap == null || layoutHitProbMap.isEmpty() || rollingUpSourceMap == null
                || rollingUpSourceMap.isEmpty()) {
            return srcLayoutsStats;
        }

        for (BigInteger layout : layoutHitProbMap.keySet()) {
            if (statistics.get(layout) != null) {
                continue;
            }
            Map<BigInteger, Pair<Long, Long>> innerRollingUpTargetMap = rollingUpSourceMap.get(layout);
            if (innerRollingUpTargetMap == null || innerRollingUpTargetMap.isEmpty()) {
                continue;
            }

            long totalEstRowCount = 0L;
            int nEffective = 0;
            boolean ifHasStats = false;
            // if ifHasStats equals true, then source layout row count = (1 - rollup ratio) * target layout row count
            //                            else source layout row count = returned row count collected directly
            for (BigInteger tgtLayout : innerRollingUpTargetMap.keySet()) {
                Pair<Long, Long> rollingUpStats = innerRollingUpTargetMap.get(tgtLayout);
                if (statistics.get(tgtLayout) != null) {
                    if (!ifHasStats) {
                        totalEstRowCount = 0L;
                        nEffective = 0;
                        ifHasStats = true;
                    }
                    double rollupRatio = calculateRollupRatio(rollingUpStats);
                    totalEstRowCount += (1 - rollupRatio) * statistics.get(tgtLayout);
                } else {
                    if (ifHasStats) {
                        continue;
                    }
                    totalEstRowCount += rollingUpStats.getSecond();
                }
                nEffective++;
            }

            if (nEffective != 0)
                srcLayoutsStats.put(layout, totalEstRowCount / nEffective);
            else
                throw new ArithmeticException("/ by zero");
        }
        srcLayoutsStats.remove(BigInteger.ZERO);
        adjustLayoutStats(srcLayoutsStats, statistics);
        return srcLayoutsStats;
    }

    /**
     * Complement row count for mandatory layouts
     * with its best parent's row count
     * */
    public static Map<BigInteger, Long> complementRowCountForLayouts(final Map<BigInteger, Long> statistics,
            Set<BigInteger> layouts) {
        Map<BigInteger, Long> result = Maps.newHashMapWithExpectedSize(layouts.size());

        // Sort entries order by row count asc
        SortedSet<Map.Entry<BigInteger, Long>> sortedStatsSet = new TreeSet<>(Comparator
                .comparingLong((Map.Entry<BigInteger, Long> o) -> o.getValue()).thenComparing(Map.Entry::getKey));
        // sortedStatsSet.addAll(statistics.entrySet()); KYLIN-3580
        for (Map.Entry<BigInteger, Long> entry : statistics.entrySet()) {
            sortedStatsSet.add(entry);
        }
        for (BigInteger layout : layouts) {
            if (statistics.get(layout) == null) {
                // Get estimate row count for mandatory layout
                for (Map.Entry<BigInteger, Long> entry : sortedStatsSet) {
                    if (isDescendant(layout, entry.getKey())) {
                        result.put(layout, entry.getValue());
                        break;
                    }
                }
            } else {
                result.put(layout, statistics.get(layout));
            }
        }

        return result;
    }

    /**
     * adjust layout row count, make sure parent not less than child
     */
    public static Map<BigInteger, Long> adjustLayoutStats(Map<BigInteger, Long> statistics) {
        Map<BigInteger, Long> ret = Maps.newHashMapWithExpectedSize(statistics.size());

        List<BigInteger> layouts = Lists.newArrayList(statistics.keySet());
        Collections.sort(layouts);

        for (BigInteger layout : layouts) {
            Long rowCount = statistics.get(layout);
            for (BigInteger childLayout : ret.keySet()) {
                if (isDescendant(childLayout, layout)) {
                    Long childRowCount = ret.get(childLayout);
                    if (rowCount < childRowCount) {
                        rowCount = childRowCount;
                    }
                }
            }
            ret.put(layout, rowCount);
        }

        return ret;
    }

    public static void adjustLayoutStats(Map<BigInteger, Long> mandatoryLayoutsWithStats,
            Map<BigInteger, Long> statistics) {
        List<BigInteger> mandatoryLayouts = Lists.newArrayList(mandatoryLayoutsWithStats.keySet());
        Collections.sort(mandatoryLayouts);

        List<BigInteger> selectedLayouts = Lists.newArrayList(statistics.keySet());
        Collections.sort(selectedLayouts);

        for (int i = 0; i < mandatoryLayouts.size(); i++) {
            BigInteger mLayout = mandatoryLayouts.get(i);
            if (statistics.get(mLayout) != null) {
                mandatoryLayoutsWithStats.put(mLayout, statistics.get(mLayout));
                continue;
            }
            int k = 0;
            // Make sure mLayout's row count larger than its children's row count in statistics
            for (; k < selectedLayouts.size(); k++) {
                BigInteger sLayout = selectedLayouts.get(k);
                if (sLayout.compareTo(mLayout) > 0) {
                    // sLayout > mLayout
                    break;
                }
                if (isDescendant(sLayout, mLayout)) {
                    Long childRowCount = statistics.get(sLayout);
                    if (childRowCount > mandatoryLayoutsWithStats.get(mLayout)) {
                        mandatoryLayoutsWithStats.put(mLayout, childRowCount);
                    }
                }
            }
            // Make sure mLayout's row count larger than its children's row count in mandatoryLayouts
            for (int j = 0; j < i; j++) {
                BigInteger cLayout = mandatoryLayouts.get(j);
                if (isDescendant(cLayout, mLayout)) {
                    Long childRowCount = mandatoryLayoutsWithStats.get(cLayout);
                    if (childRowCount > mandatoryLayoutsWithStats.get(mLayout)) {
                        mandatoryLayoutsWithStats.put(mLayout, childRowCount);
                    }
                }
            }
            // Make sure mLayout's row count lower than its parents' row count in statistics
            for (; k < selectedLayouts.size(); k++) {
                BigInteger sLayout = selectedLayouts.get(k);
                if (isDescendant(mLayout, sLayout)) {
                    Long parentRowCount = statistics.get(sLayout);
                    if (parentRowCount < mandatoryLayoutsWithStats.get(mLayout)) {
                        mandatoryLayoutsWithStats.put(mLayout, parentRowCount);
                    }
                }
            }
        }
    }

    public static Map<BigInteger, List<BigInteger>> createDirectChildrenCache(final Set<BigInteger> layoutSet) {
        /*
         * Sort the list by ascending order:
         * */
        final List<BigInteger> layoutList = Lists.newArrayList(layoutSet);
        Collections.sort(layoutList);
        /*
         * Sort the list by ascending order:
         * 1. the more bit count of its value, the bigger
         * 2. the larger of its value, the bigger
         * */
        List<Integer> layerIdxList = Lists.newArrayListWithExpectedSize(layoutList.size());
        for (int i = 0; i < layoutList.size(); i++) {
            layerIdxList.add(i);
        }
        layerIdxList.sort((i1, i2) -> {
            BigInteger o1 = layoutList.get(i1);
            BigInteger o2 = layoutList.get(i2);
            int nBitDiff = o1.bitCount() - o2.bitCount();
            if (nBitDiff != 0) {
                return nBitDiff;
            }
            return o1.compareTo(o2);
        });

        /*
         * Construct an index array for pointing the position in layerIdxList
         * (layerLayoutList is for speeding up continuous iteration)
         * */
        int[] toLayerIdxArray = new int[layerIdxList.size()];
        final List<BigInteger> layerLayoutList = Lists.newArrayListWithExpectedSize(layoutList.size());
        for (int i = 0; i < layerIdxList.size(); i++) {
            int layoutIdx = layerIdxList.get(i);
            toLayerIdxArray[layoutIdx] = i;
            layerLayoutList.add(layoutList.get(layoutIdx));
        }

        int[] previousLayerLastIdxArray = new int[layerIdxList.size()];
        int currentBitCount = 0;
        int previousLayerLastIdx = -1;
        for (int i = 0; i < layerIdxList.size(); i++) {
            int layoutIdx = layerIdxList.get(i);
            // get the bit count from the biginteger api
            int nBits = layoutList.get(layoutIdx).bitCount();
            if (nBits > currentBitCount) {
                currentBitCount = nBits;
                previousLayerLastIdx = i - 1;
            }
            previousLayerLastIdxArray[i] = previousLayerLastIdx;
        }

        Map<BigInteger, List<BigInteger>> directChildrenCache = Maps.newHashMap();
        for (int i = 0; i < layoutList.size(); i++) {
            BigInteger currentLayout = layoutList.get(i);
            LinkedList<BigInteger> directChildren = Lists.newLinkedList();
            int lastLayerIdx = previousLayerLastIdxArray[toLayerIdxArray[i]];
            /*
             * Choose one of the two scan strategies
             * 1. layouts are sorted by its value, like 1,2,3,4,...
             * 2. layouts are layered and sorted, like 1,2,4,8,...,3,5,...
             */
            if (i - 1 <= lastLayerIdx) {
                // 1. Adding layout by descending order
                for (int j = i - 1; j >= 0; j--) {
                    checkAndAddDirectChild(directChildren, currentLayout, layoutList.get(j));
                }
            } else {
                /*
                 * 1. Adding layout by descending order
                 * 2. Check from lower layout layer
                 */
                for (int j = lastLayerIdx; j >= 0; j--) {
                    checkAndAddDirectChild(directChildren, currentLayout, layerLayoutList.get(j));
                }
            }
            directChildrenCache.put(currentLayout, directChildren);
        }
        return directChildrenCache;
    }

    private static void checkAndAddDirectChild(List<BigInteger> directChildren, BigInteger currentLayout,
            BigInteger checkedLayout) {
        if (isDescendant(checkedLayout, currentLayout)) {
            boolean ifDirectChild = true;
            for (BigInteger directChild : directChildren) {
                if (isDescendant(checkedLayout, directChild)) {
                    ifDirectChild = false;
                    break;
                }
            }
            if (ifDirectChild) {
                directChildren.add(checkedLayout);
            }
        }
    }

    private static boolean isDescendant(BigInteger layoutToCheck, BigInteger parentLayout) {
        return (layoutToCheck.and(parentLayout)).equals(layoutToCheck);
    }

    private static double calculateRollupRatio(Pair<Long, Long> rollupStats) {
        double rollupInputCount = (double) rollupStats.getFirst() + rollupStats.getSecond();
        return rollupInputCount == 0 ? 0 : 1.0 * rollupStats.getFirst() / rollupInputCount;
    }
}
