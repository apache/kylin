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
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.kylin.common.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class CuboidStatsUtil {

    private static final Logger logger = LoggerFactory.getLogger(CuboidStatsUtil.class);

    private CuboidStatsUtil() {
        throw new IllegalStateException("Class CuboidStatsUtil is an utility class !");
    }

    /**
     * According to the cuboid hit frequencies and query uncertainty ratio
     * calculate each cuboid hit probability
     * @param selectionCuboidSet subset of cuboid domain which needs probability
     * @param nTotalCuboids number of cuboids needs to be considered, mainly for each cuboid's uncertainty weight
     * */
    public static Map<BigInteger, Double> calculateCuboidHitProbability(Set<BigInteger> selectionCuboidSet,
            Map<BigInteger, Long> hitFrequencyMap, BigInteger nTotalCuboids, double queryUncertaintyRatio) {
        Map<BigInteger, Double> cuboidHitProbabilityMap = Maps.newHashMapWithExpectedSize(selectionCuboidSet.size());
        if (hitFrequencyMap == null || hitFrequencyMap.isEmpty()) {
            double value = BigDecimal.valueOf(1.0).divide(new BigDecimal(nTotalCuboids), 15, BigDecimal.ROUND_HALF_EVEN)
                    .doubleValue();
            for (BigInteger cuboid : selectionCuboidSet) {
                cuboidHitProbabilityMap.put(cuboid, value);
            }
        } else {
            long totalHitFrequency = 0L;
            for (Map.Entry<BigInteger, Long> hitFrequency : hitFrequencyMap.entrySet()) {
                totalHitFrequency += hitFrequency.getValue();
            }

            final double unitUncertainProb = BigDecimal.valueOf(queryUncertaintyRatio)
                    .divide(new BigDecimal(nTotalCuboids), 15, BigDecimal.ROUND_HALF_EVEN).doubleValue();
            for (BigInteger cuboid : selectionCuboidSet) {
                //Calculate hit probability for each cuboid
                if (hitFrequencyMap.get(cuboid) != null) {
                    if (totalHitFrequency != 0)
                        cuboidHitProbabilityMap.put(cuboid, unitUncertainProb
                                + (1 - queryUncertaintyRatio) * hitFrequencyMap.get(cuboid) / totalHitFrequency);
                    else
                        throw new ArithmeticException("/ by zero");
                } else {
                    cuboidHitProbabilityMap.put(cuboid, unitUncertainProb);
                }
            }
        }

        return cuboidHitProbabilityMap;
    }

    /**
     * @param statistics for cuboid row count
     * @param rollingUpSourceMap the key of the outer map is source cuboid,
     *                           the key of the inner map is target cuboid,
     *                                  if cube is optimized multiple times, target cuboid may change
     *                           the first element of the pair is the rollup row count
     *                           the second element of the pair is the return row count
     * @return source cuboids with estimated row count
     */
    public static Map<BigInteger, Long> generateSourceCuboidStats(Map<BigInteger, Long> statistics,
            Map<BigInteger, Double> cuboidHitProbabilityMap,
            Map<BigInteger, Map<BigInteger, Pair<Long, Long>>> rollingUpSourceMap) {
        Map<BigInteger, Long> srcCuboidsStats = Maps.newHashMap();
        if (cuboidHitProbabilityMap == null || cuboidHitProbabilityMap.isEmpty() || rollingUpSourceMap == null
                || rollingUpSourceMap.isEmpty()) {
            return srcCuboidsStats;
        }

        for (BigInteger cuboid : cuboidHitProbabilityMap.keySet()) {
            if (statistics.get(cuboid) != null) {
                continue;
            }
            Map<BigInteger, Pair<Long, Long>> innerRollingUpTargetMap = rollingUpSourceMap.get(cuboid);
            if (innerRollingUpTargetMap == null || innerRollingUpTargetMap.isEmpty()) {
                continue;
            }

            long totalEstRowCount = 0L;
            int nEffective = 0;
            boolean ifHasStats = false;
            // if ifHasStats equals true, then source cuboid row count = (1 - rollup ratio) * target cuboid row count
            //                            else source cuboid row count = returned row count collected directly
            for (BigInteger tgtCuboid : innerRollingUpTargetMap.keySet()) {
                Pair<Long, Long> rollingupStats = innerRollingUpTargetMap.get(tgtCuboid);
                if (statistics.get(tgtCuboid) != null) {
                    if (!ifHasStats) {
                        totalEstRowCount = 0L;
                        nEffective = 0;
                        ifHasStats = true;
                    }
                    double rollupRatio = calculateRollupRatio(rollingupStats);
                    totalEstRowCount += (1 - rollupRatio) * statistics.get(tgtCuboid);
                    nEffective++;
                } else {
                    if (ifHasStats) {
                        continue;
                    }
                    totalEstRowCount += rollingupStats.getSecond();
                    nEffective++;
                }
            }

            if (nEffective != 0)
                srcCuboidsStats.put(cuboid, totalEstRowCount / nEffective);
            else
                throw new ArithmeticException("/ by zero");
        }
        srcCuboidsStats.remove(BigInteger.ZERO);
        adjustCuboidStats(srcCuboidsStats, statistics);
        return srcCuboidsStats;
    }

    /**
     * Complement row count for mandatory cuboids
     * with its best parent's row count
     * */
    public static Map<BigInteger, Long> complementRowCountForCuboids(final Map<BigInteger, Long> statistics,
            Set<BigInteger> cuboids) {
        Map<BigInteger, Long> result = Maps.newHashMapWithExpectedSize(cuboids.size());

        // Sort entries order by row count asc
        SortedSet<Map.Entry<BigInteger, Long>> sortedStatsSet = new TreeSet<>(
                new Comparator<Map.Entry<BigInteger, Long>>() {
                    public int compare(Map.Entry<BigInteger, Long> o1, Map.Entry<BigInteger, Long> o2) {
                        int ret = o1.getValue().compareTo(o2.getValue());
                        return ret == 0 ? o1.getKey().compareTo(o2.getKey()) : ret;
                    }
                });
        //sortedStatsSet.addAll(statistics.entrySet()); KYLIN-3580
        for (Map.Entry<BigInteger, Long> entry : statistics.entrySet()) {
            sortedStatsSet.add(entry);
        }
        for (BigInteger cuboid : cuboids) {
            if (statistics.get(cuboid) == null) {
                // Get estimate row count for mandatory cuboid
                for (Map.Entry<BigInteger, Long> entry : sortedStatsSet) {
                    if (isDescendant(cuboid, entry.getKey())) {
                        result.put(cuboid, entry.getValue());
                        break;
                    }
                }
            } else {
                result.put(cuboid, statistics.get(cuboid));
            }
        }

        return result;
    }

    /**
     * adjust cuboid row count, make sure parent not less than child
     */
    public static Map<BigInteger, Long> adjustCuboidStats(Map<BigInteger, Long> statistics) {
        Map<BigInteger, Long> ret = Maps.newHashMapWithExpectedSize(statistics.size());

        List<BigInteger> cuboids = Lists.newArrayList(statistics.keySet());
        Collections.sort(cuboids);

        for (BigInteger cuboid : cuboids) {
            Long rowCount = statistics.get(cuboid);
            for (BigInteger childCuboid : ret.keySet()) {
                if (isDescendant(childCuboid, cuboid)) {
                    Long childRowCount = ret.get(childCuboid);
                    if (rowCount < childRowCount) {
                        rowCount = childRowCount;
                    }
                }
            }
            ret.put(cuboid, rowCount);
        }

        return ret;
    }

    public static void adjustCuboidStats(Map<BigInteger, Long> mandatoryCuboidsWithStats,
            Map<BigInteger, Long> statistics) {
        List<BigInteger> mandatoryCuboids = Lists.newArrayList(mandatoryCuboidsWithStats.keySet());
        Collections.sort(mandatoryCuboids);

        List<BigInteger> selectedCuboids = Lists.newArrayList(statistics.keySet());
        Collections.sort(selectedCuboids);

        for (int i = 0; i < mandatoryCuboids.size(); i++) {
            BigInteger mCuboid = mandatoryCuboids.get(i);
            if (statistics.get(mCuboid) != null) {
                mandatoryCuboidsWithStats.put(mCuboid, statistics.get(mCuboid));
                continue;
            }
            int k = 0;
            // Make sure mCuboid's row count larger than its children's row count in statistics
            for (; k < selectedCuboids.size(); k++) {
                BigInteger sCuboid = selectedCuboids.get(k);
                if (sCuboid.compareTo(mCuboid) > 0) {
                    // sCuboid > mCuboid
                    break;
                }
                if (isDescendant(sCuboid, mCuboid)) {
                    Long childRowCount = statistics.get(sCuboid);
                    if (childRowCount > mandatoryCuboidsWithStats.get(mCuboid)) {
                        mandatoryCuboidsWithStats.put(mCuboid, childRowCount);
                    }
                }
            }
            // Make sure mCuboid's row count larger than its children's row count in mandatoryCuboids
            for (int j = 0; j < i; j++) {
                BigInteger cCuboid = mandatoryCuboids.get(j);
                if (isDescendant(cCuboid, mCuboid)) {
                    Long childRowCount = mandatoryCuboidsWithStats.get(cCuboid);
                    if (childRowCount > mandatoryCuboidsWithStats.get(mCuboid)) {
                        mandatoryCuboidsWithStats.put(mCuboid, childRowCount);
                    }
                }
            }
            // Make sure mCuboid's row count lower than its parents' row count in statistics
            for (; k < selectedCuboids.size(); k++) {
                BigInteger sCuboid = selectedCuboids.get(k);
                if (isDescendant(mCuboid, sCuboid)) {
                    Long parentRowCount = statistics.get(sCuboid);
                    if (parentRowCount < mandatoryCuboidsWithStats.get(mCuboid)) {
                        mandatoryCuboidsWithStats.put(mCuboid, parentRowCount);
                    }
                }
            }
        }
    }

    public static Map<BigInteger, List<BigInteger>> createDirectChildrenCache(final Set<BigInteger> cuboidSet) {
        /**
         * Sort the list by ascending order:
         * */
        final List<BigInteger> cuboidList = Lists.newArrayList(cuboidSet);
        Collections.sort(cuboidList);
        /**
         * Sort the list by ascending order:
         * 1. the more bit count of its value, the bigger
         * 2. the larger of its value, the bigger
         * */
        List<Integer> layerIdxList = Lists.newArrayListWithExpectedSize(cuboidList.size());
        for (int i = 0; i < cuboidList.size(); i++) {
            layerIdxList.add(i);
        }
        Collections.sort(layerIdxList, new Comparator<Integer>() {
            @Override
            public int compare(Integer i1, Integer i2) {
                BigInteger o1 = cuboidList.get(i1);
                BigInteger o2 = cuboidList.get(i2);
                int nBitDiff = o1.bitCount() - o2.bitCount();
                if (nBitDiff != 0) {
                    return nBitDiff;
                }
                return o1.compareTo(o2);
            }
        });
        /**
         * Construct an index array for pointing the position in layerIdxList
         * (layerCuboidList is for speeding up continuous iteration)
         * */
        int[] toLayerIdxArray = new int[layerIdxList.size()];
        final List<BigInteger> layerCuboidList = Lists.newArrayListWithExpectedSize(cuboidList.size());
        for (int i = 0; i < layerIdxList.size(); i++) {
            int cuboidIdx = layerIdxList.get(i);
            toLayerIdxArray[cuboidIdx] = i;
            layerCuboidList.add(cuboidList.get(cuboidIdx));
        }

        int[] previousLayerLastIdxArray = new int[layerIdxList.size()];
        int currentBitCount = 0;
        int previousLayerLastIdx = -1;
        for (int i = 0; i < layerIdxList.size(); i++) {
            int cuboidIdx = layerIdxList.get(i);
            // get bit count from the biginteger api
            int nBits = cuboidList.get(cuboidIdx).bitCount();
            if (nBits > currentBitCount) {
                currentBitCount = nBits;
                previousLayerLastIdx = i - 1;
            }
            previousLayerLastIdxArray[i] = previousLayerLastIdx;
        }

        Map<BigInteger, List<BigInteger>> directChildrenCache = Maps.newHashMap();
        for (int i = 0; i < cuboidList.size(); i++) {
            BigInteger currentCuboid = cuboidList.get(i);
            LinkedList<BigInteger> directChildren = Lists.newLinkedList();
            int lastLayerIdx = previousLayerLastIdxArray[toLayerIdxArray[i]];
            /**
             * Choose one of the two scan strategies
             * 1. cuboids are sorted by its value, like 1,2,3,4,...
             * 2. cuboids are layered and sorted, like 1,2,4,8,...,3,5,...
             * */
            if (i - 1 <= lastLayerIdx) {
                /**
                 * 1. Adding cuboid by descending order
                 * */
                for (int j = i - 1; j >= 0; j--) {
                    checkAndAddDirectChild(directChildren, currentCuboid, cuboidList.get(j));
                }
            } else {
                /**
                 * 1. Adding cuboid by descending order
                 * 2. Check from lower cuboid layer
                 * */
                for (int j = lastLayerIdx; j >= 0; j--) {
                    checkAndAddDirectChild(directChildren, currentCuboid, layerCuboidList.get(j));
                }
            }
            directChildrenCache.put(currentCuboid, directChildren);
        }
        return directChildrenCache;
    }

    private static void checkAndAddDirectChild(List<BigInteger> directChildren, BigInteger currentCuboid,
            BigInteger checkedCuboid) {
        if (isDescendant(checkedCuboid, currentCuboid)) {
            boolean ifDirectChild = true;
            for (BigInteger directChild : directChildren) {
                if (isDescendant(checkedCuboid, directChild)) {
                    ifDirectChild = false;
                    break;
                }
            }
            if (ifDirectChild) {
                directChildren.add(checkedCuboid);
            }
        }
    }

    private static boolean isDescendant(BigInteger cuboidToCheck, BigInteger parentCuboid) {
        return (cuboidToCheck.and(parentCuboid)).equals(cuboidToCheck);
    }

    private static double calculateRollupRatio(Pair<Long, Long> rollupStats) {
        double rollupInputCount = (double) rollupStats.getFirst() + rollupStats.getSecond();
        return rollupInputCount == 0 ? 0 : 1.0 * rollupStats.getFirst() / rollupInputCount;
    }
}
