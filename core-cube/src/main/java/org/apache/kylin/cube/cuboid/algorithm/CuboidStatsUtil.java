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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import com.google.common.annotations.VisibleForTesting;
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

    /** Using dynamic programming to use extra space to reduce repetitive computation*/
    public static Map<Long, Set<Long>> createAllDescendantsCache(final Set<Long> cuboidSet) {
        List<Long> latticeCuboidList = Lists.newArrayList(cuboidSet);
        Collections.sort(latticeCuboidList);

        Map<Long, Set<Long>> allDescendantsCache = Maps.newHashMap();
        Set<Long> preNoneDescendants = Sets.newHashSet();
        for (int i = 0; i < latticeCuboidList.size(); i++) {
            Long currentCuboid = latticeCuboidList.get(i);
            Set<Long> descendants = Sets.newHashSet(currentCuboid);
            Set<Long> curNoneDescendants = Sets.newHashSet();
            if (i > 0) {
                long preCuboid = latticeCuboidList.get(i - 1);
                if (isDescendant(preCuboid, currentCuboid)) {
                    descendants.addAll(allDescendantsCache.get(preCuboid));
                } else {
                    curNoneDescendants.add(preCuboid);
                    for (long cuboidToCheck : allDescendantsCache.get(preCuboid)) {
                        if (isDescendant(cuboidToCheck, currentCuboid)) {
                            descendants.addAll(allDescendantsCache.get(cuboidToCheck));
                        }
                    }
                }
            }
            for (long cuboidToCheck : preNoneDescendants) {
                if (isDescendant(cuboidToCheck, currentCuboid)) {
                    descendants.addAll(allDescendantsCache.get(cuboidToCheck));
                } else {
                    curNoneDescendants.add(cuboidToCheck);
                }
            }

            allDescendantsCache.put(currentCuboid, descendants);
            preNoneDescendants = curNoneDescendants;
        }

        return allDescendantsCache;
    }

    @VisibleForTesting
    static Map<Long, Set<Long>> createAllDescendantsCache2(final Set<Long> cuboidSet) {
        List<Long> latticeCuboidList = Lists.newArrayList(cuboidSet);

        Map<Long, Set<Long>> allDescendantsCache = Maps.newHashMap();
        for (int i = 0; i < latticeCuboidList.size(); i++) {
            Long currentCuboid = latticeCuboidList.get(i);
            Set<Long> descendantSet = Sets.newHashSet(currentCuboid);
            for (int j = 0; j < i; j++) {
                Long checkCuboid = latticeCuboidList.get(j);
                if (isDescendant(checkCuboid, currentCuboid)) {
                    descendantSet.add(checkCuboid);
                }
            }
            allDescendantsCache.put(currentCuboid, descendantSet);
        }
        return allDescendantsCache;
    }

    public static boolean isDescendant(long cuboidToCheck, long parentCuboid) {
        return (cuboidToCheck & parentCuboid) == cuboidToCheck;
    }
}
