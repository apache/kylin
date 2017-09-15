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

import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class CuboidStatsUtil {

    /**
     * For generating mandatory cuboids,
     * a cuboid is mandatory if the expectation of rolling up count exceeds a threshold
     * */
    public static Set<Long> generateMandatoryCuboidSet(Map<Long, Long> statistics, Map<Long, Long> hitFrequencyMap,
            Map<Long, Map<Long, Long>> rollingUpCountSourceMap, final long rollUpThresholdForMandatory) {
        Set<Long> mandatoryCuboidSet = Sets.newHashSet();
        if (hitFrequencyMap == null || hitFrequencyMap.isEmpty() || rollingUpCountSourceMap == null
                || rollingUpCountSourceMap.isEmpty()) {
            return mandatoryCuboidSet;
        }
        long totalHitFrequency = 0L;
        for (long hitFrequency : hitFrequencyMap.values()) {
            totalHitFrequency += hitFrequency;
        }

        if (totalHitFrequency == 0) {
            return mandatoryCuboidSet;
        }

        for (Map.Entry<Long, Long> hitFrequency : hitFrequencyMap.entrySet()) {
            long cuboid = hitFrequency.getKey();
            if (statistics.get(cuboid) != null) {
                continue;
            }
            if (rollingUpCountSourceMap.get(cuboid) == null || rollingUpCountSourceMap.get(cuboid).isEmpty()) {
                continue;
            }
            long totalEstScanCount = 0L;
            for (long estScanCount : rollingUpCountSourceMap.get(cuboid).values()) {
                totalEstScanCount += estScanCount;
            }
            totalEstScanCount /= rollingUpCountSourceMap.get(cuboid).size();
            if ((hitFrequency.getValue() * 1.0 / totalHitFrequency)
                    * totalEstScanCount >= rollUpThresholdForMandatory) {
                mandatoryCuboidSet.add(cuboid);
            }
        }
        return mandatoryCuboidSet;
    }

    /**
     * Complement row count for mandatory cuboids
     * with its best parent's row count
     * */
    public static void complementRowCountForMandatoryCuboids(Map<Long, Long> statistics, long baseCuboid,
            Set<Long> mandatoryCuboidSet) {
        // Sort entries order by row count asc
        SortedSet<Map.Entry<Long, Long>> sortedStatsSet = new TreeSet<Map.Entry<Long, Long>>(
                new Comparator<Map.Entry<Long, Long>>() {
                    public int compare(Map.Entry<Long, Long> o1, Map.Entry<Long, Long> o2) {
                        return o1.getValue().compareTo(o2.getValue());
                    }
                });
        sortedStatsSet.addAll(statistics.entrySet());
        for (Long cuboid : mandatoryCuboidSet) {
            if (statistics.get(cuboid) == null) {
                // Get estimate row count for mandatory cuboid
                long tmpRowCount = -1;
                for (Map.Entry<Long, Long> entry : sortedStatsSet) {
                    if (isDescendant(cuboid, entry.getKey())) {
                        tmpRowCount = entry.getValue();
                    }
                }
                statistics.put(cuboid, tmpRowCount < 0 ? statistics.get(baseCuboid) : tmpRowCount);
            }
        }
    }

    public static Map<Long, List<Long>> createDirectChildrenCache(final Set<Long> cuboidSet) {
        /**
         * Sort the list by ascending order:
         * */
        final List<Long> cuboidList = Lists.newArrayList(cuboidSet);
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
                Long o1 = cuboidList.get(i1);
                Long o2 = cuboidList.get(i2);
                int nBitDiff = Long.bitCount(o1) - Long.bitCount(o2);
                if (nBitDiff != 0) {
                    return nBitDiff;
                }
                return Long.compare(o1, o2);
            }
        });
        /**
         * Construct an index array for pointing the position in layerIdxList
         * (layerCuboidList is for speeding up continuous iteration)
         * */
        int[] toLayerIdxArray = new int[layerIdxList.size()];
        final List<Long> layerCuboidList = Lists.newArrayListWithExpectedSize(cuboidList.size());
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
            int nBits = Long.bitCount(cuboidList.get(cuboidIdx));
            if (nBits > currentBitCount) {
                currentBitCount = nBits;
                previousLayerLastIdx = i - 1;
            }
            previousLayerLastIdxArray[i] = previousLayerLastIdx;
        }

        Map<Long, List<Long>> directChildrenCache = Maps.newHashMap();
        for (int i = 0; i < cuboidList.size(); i++) {
            Long currentCuboid = cuboidList.get(i);
            LinkedList<Long> directChildren = Lists.newLinkedList();
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

    private static void checkAndAddDirectChild(List<Long> directChildren, Long currentCuboid, Long checkedCuboid) {
        if (isDescendant(checkedCuboid, currentCuboid)) {
            boolean ifDirectChild = true;
            for (long directChild : directChildren) {
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

    public static boolean isDescendant(long cuboidToCheck, long parentCuboid) {
        return (cuboidToCheck & parentCuboid) == cuboidToCheck;
    }
}
