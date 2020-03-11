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

package org.apache.kylin.cube.cuboid;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kylin.cube.cuboid.algorithm.CuboidStatsUtil;

import org.apache.kylin.shaded.com.google.common.base.Preconditions;
import org.apache.kylin.shaded.com.google.common.collect.Lists;
import org.apache.kylin.shaded.com.google.common.collect.Maps;

public class CuboidUtil {

    public static Integer[][] getCuboidBitSet(Long[] cuboidIds, int nRowKey) {
        Preconditions.checkArgument(nRowKey < Long.SIZE,
                "the size of row key could not be large than " + (Long.SIZE - 1));

        Integer[][] allCuboidsBitSet = new Integer[cuboidIds.length][];

        for (int j = 0; j < cuboidIds.length; j++) {
            Long cuboidId = cuboidIds[j];

            allCuboidsBitSet[j] = new Integer[Long.bitCount(cuboidId)];

            long mask = 1L << (nRowKey - 1);
            int position = 0;
            for (int i = 0; i < nRowKey; i++) {
                if ((mask & cuboidId) > 0) {
                    allCuboidsBitSet[j][position] = i;
                    position++;
                }
                mask = mask >> 1;
            }
        }
        return allCuboidsBitSet;
    }

    public static int getLongestDepth(Set<Long> cuboidSet) {
        Map<Long, List<Long>> directChildrenCache = CuboidStatsUtil.createDirectChildrenCache(cuboidSet);
        List<Long> cuboids = Lists.newArrayList(cuboidSet);
        Collections.sort(cuboids, new Comparator<Long>() {
            @Override
            public int compare(Long o1, Long o2) {
                return -Long.compare(o1, o2);
            }
        });

        int longestDepth = 0;
        Map<Long, Integer> cuboidDepthMap = Maps.newHashMap();
        for (Long cuboid : cuboids) {
            int parentDepth = cuboidDepthMap.get(cuboid) == null ? 0 : cuboidDepthMap.get(cuboid);
            for (Long childCuboid : directChildrenCache.get(cuboid)) {
                if (cuboidDepthMap.get(childCuboid) == null || cuboidDepthMap.get(childCuboid) < parentDepth + 1) {
                    cuboidDepthMap.put(childCuboid, parentDepth + 1);
                    if (longestDepth < parentDepth + 1) {
                        longestDepth = parentDepth + 1;
                    }
                }
            }
        }

        return longestDepth;
    }
}
