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

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class CuboidStatsUtilTest {

    /**
     *                 (11111111)
     *               /     |      \
     *      10011111   (11101111)  00110010
     *           \         |         /
     *             \   11000111     /
     *               \     |       /
     *                 00000110   /
     *              /      |     /
     *         00000100  00000010
     * */
    private Set<Long> generateCuboidSet() {
        return Sets.newHashSet(255L, 159L, 239L, 50L, 199L, 6L, 4L, 2L);
    }

    private Map<Long, Long> simulateCount() {
        Map<Long, Long> countMap = Maps.newHashMap();

        countMap.put(255L, 10000L);
        countMap.put(159L, 10000L);
        countMap.put(50L, 10000L);
        countMap.put(199L, 10000L);
        countMap.put(6L, 10000L);
        countMap.put(4L, 10000L);
        countMap.put(2L, 10000L);

        return countMap;
    }

    private Map<Long, Long> simulateHitFrequency() {
        Map<Long, Long> hitFrequencyMap = Maps.newHashMap();

        long totalHitFrequency = 10000L;

        hitFrequencyMap.put(239L, (long) (totalHitFrequency * 0.5));
        hitFrequencyMap.put(50L, (long) (totalHitFrequency * 0.2));
        hitFrequencyMap.put(2L, (long) (totalHitFrequency * 0.25));
        hitFrequencyMap.put(178L, (long) (totalHitFrequency * 0.05));

        return hitFrequencyMap;
    }

    private Map<Long, Map<Long, Long>> simulateRollingUpCount() {
        Map<Long, Map<Long, Long>> rollingUpCountMap = Maps.newLinkedHashMap();

        rollingUpCountMap.put(239L, new HashMap<Long, Long>() {
            {
                put(255L, 4000L);
            }
        });

        rollingUpCountMap.put(178L, new HashMap<Long, Long>() {
            {
                put(255L, 5000L);
            }
        });

        return rollingUpCountMap;
    }

    @Test
    public void isDescendantTest() {
        Assert.assertTrue(CuboidStatsUtil.isDescendant(6L, 239L));
        Assert.assertTrue(!CuboidStatsUtil.isDescendant(4L, 50L));
    }

    @Test
    public void generateMandatoryCuboidSetTest() {
        Set<Long> mandatoryCuboidSet = CuboidStatsUtil.generateMandatoryCuboidSet(simulateCount(),
                simulateHitFrequency(), simulateRollingUpCount(), 1000L);
        Assert.assertTrue(mandatoryCuboidSet.contains(239L));
        Assert.assertTrue(!mandatoryCuboidSet.contains(178L));
    }

    @Test
    public void complementRowCountForMandatoryCuboidsTest() {
        Map<Long, Long> countMap = simulateCount();
        Set<Long> mandatoryCuboidSet = CuboidStatsUtil.generateMandatoryCuboidSet(countMap, simulateHitFrequency(),
                simulateRollingUpCount(), 1000L);
        for (long mandatoryCuboid : mandatoryCuboidSet) {
            Assert.assertNull(countMap.get(mandatoryCuboid));
        }
        CuboidStatsUtil.complementRowCountForMandatoryCuboids(countMap, 255L, mandatoryCuboidSet);
        for (long mandatoryCuboid : mandatoryCuboidSet) {
            Assert.assertNotNull(countMap.get(mandatoryCuboid));
        }
        Assert.assertTrue(countMap.get(239L) == 10000L);
    }

    @Test
    public void createAllDescendantsCacheTest() {
        Set<Long> cuboidSet = generateCuboidSet();
        Map<Long, Set<Long>> allDescendantsCache = CuboidStatsUtil.createAllDescendantsCache(cuboidSet);

        Assert.assertTrue(allDescendantsCache.get(255L).containsAll(cuboidSet));

        Assert.assertTrue(allDescendantsCache.get(239L).size() == 5);

        Assert.assertTrue(allDescendantsCache.get(50L).containsAll(Sets.newHashSet(50L, 2L)));
        Assert.assertTrue(!allDescendantsCache.get(50L).contains(4L));
    }

    private Set<Long> generateMassCuboidSet() {
        Set<Long> cuboidSet = Sets.newHashSet();
        long maxCuboid = (1L << 16);
        for (long i = 1; i < maxCuboid; i++) {
            cuboidSet.add(i);
        }
        return cuboidSet;
    }

    @Test
    public void createAllDescendantsCacheStressTest() {
        Stopwatch sw = new Stopwatch();
        sw.start();
        Set<Long> cuboidSet = generateMassCuboidSet();
        System.out.println("Time elapsed for creating sorted cuboid list: " + sw.elapsed(TimeUnit.MILLISECONDS));
        sw.reset();
        sw.start();
        CuboidStatsUtil.createAllDescendantsCache(cuboidSet);
        System.out.println("Time elapsed for creating descendants cache: " + sw.elapsed(TimeUnit.MILLISECONDS));
        sw.reset();
        sw.start();
        CuboidStatsUtil.createAllDescendantsCache2(cuboidSet);
        System.out.println("Time elapsed for creating descendants cache2: " + sw.elapsed(TimeUnit.MILLISECONDS));
    }
}
