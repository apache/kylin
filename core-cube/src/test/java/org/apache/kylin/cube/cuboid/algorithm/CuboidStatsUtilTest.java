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
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kylin.common.util.Pair;
import org.junit.Assert;
import org.junit.Test;

import org.apache.kylin.shaded.com.google.common.base.Stopwatch;
import org.apache.kylin.shaded.com.google.common.collect.Lists;
import org.apache.kylin.shaded.com.google.common.collect.Maps;
import org.apache.kylin.shaded.com.google.common.collect.Sets;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

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

    private final static long baseCuboidId = 255L;

    private Set<Long> generateCuboidSet() {
        return Sets.newHashSet(255L, 159L, 239L, 50L, 199L, 6L, 4L, 2L);
    }

    private Map<Long, Long> simulateCount() {
        Map<Long, Long> countMap = Maps.newHashMap();

        countMap.put(255L, 10000L);
        countMap.put(159L, 10000L);
        countMap.put(50L, 800L);
        countMap.put(199L, 200L);
        countMap.put(6L, 60L);
        countMap.put(4L, 40L);
        countMap.put(2L, 20L);

        return countMap;
    }

    private Map<Long, Long> simulateHitFrequency() {
        Map<Long, Long> hitFrequencyMap = Maps.newHashMap();

        long totalHitFrequency = 10000L;

        hitFrequencyMap.put(239L, (long) (totalHitFrequency * 0.2));
        hitFrequencyMap.put(50L, (long) (totalHitFrequency * 0.2));
        hitFrequencyMap.put(2L, (long) (totalHitFrequency * 0.24));
        hitFrequencyMap.put(178L, (long) (totalHitFrequency * 0.05));
        hitFrequencyMap.put(187L, (long) (totalHitFrequency * 0.3));
        hitFrequencyMap.put(0L, (long) (totalHitFrequency * 0.01));

        return hitFrequencyMap;
    }

    private Map<Long, Double> simulateHitProbability(long nCuboids) {
        Map<Long, Long> hitFrequencyMap = simulateHitFrequency();
        return CuboidStatsUtil.calculateCuboidHitProbability(hitFrequencyMap.keySet(), hitFrequencyMap, nCuboids,
                CuboidStats.WEIGHT_FOR_UN_QUERY);
    }

    private Map<Long, Map<Long, Pair<Long, Long>>> simulateRollingUpCount() {
        Map<Long, Map<Long, Pair<Long, Long>>> rollingUpCountMap = Maps.newLinkedHashMap();

        rollingUpCountMap.put(239L, new HashMap<Long, Pair<Long, Long>>() {
            {
                put(255L, new Pair<>(990L, 10L));
            }
        });

        rollingUpCountMap.put(178L, new HashMap<Long, Pair<Long, Long>>() {
            {
                put(255L, new Pair<>(4999L, 1L));
            }
        });

        rollingUpCountMap.put(187L, new HashMap<Long, Pair<Long, Long>>() {
            {
                put(251L, new Pair<>(3000L, 1000L));
            }
        });

        rollingUpCountMap.put(0L, new HashMap<Long, Pair<Long, Long>>() {
            {
                put(2L, new Pair<>(19L, 1L));
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
        Map<Long, Long> srcCuboidSet = CuboidStatsUtil.generateSourceCuboidStats(simulateCount(),
                simulateHitProbability(baseCuboidId), simulateRollingUpCount());

        Assert.assertTrue(srcCuboidSet.get(239L) == 200L);
        Assert.assertTrue(srcCuboidSet.get(187L) == 1000L);
        Assert.assertTrue(srcCuboidSet.get(178L) == 800L);

        Assert.assertTrue(!srcCuboidSet.containsKey(0L));
    }

    @Test
    public void complementRowCountForMandatoryCuboidsTest() {
        Map<Long, Long> countMap = simulateCount();
        Map<Long, Long> srcCuboidsStats = CuboidStatsUtil.generateSourceCuboidStats(countMap,
                simulateHitProbability(baseCuboidId), simulateRollingUpCount());
        for (long mandatoryCuboid : srcCuboidsStats.keySet()) {
            Assert.assertNull(countMap.get(mandatoryCuboid));
        }
        Assert.assertTrue(srcCuboidsStats.get(239L) == 200L);

        Map<Long, Long> mandatoryCuboidsWithStats2 = Maps.newHashMap();
        mandatoryCuboidsWithStats2.put(215L, countMap.get(255L));
        mandatoryCuboidsWithStats2.put(34L, countMap.get(50L));
        Assert.assertEquals(mandatoryCuboidsWithStats2,
                CuboidStatsUtil.complementRowCountForCuboids(countMap, mandatoryCuboidsWithStats2.keySet()));
    }

    @Test
    public void testAdjustMandatoryCuboidStats() {
        Map<Long, Long> statistics = Maps.newHashMap();
        statistics.put(60160L, 1212L);

        Map<Long, Long> cuboidsWithStats = Maps.newHashMap();
        cuboidsWithStats.put(65280L, 1423L);
        cuboidsWithStats.put(63232L, 2584421L);
        cuboidsWithStats.put(61184L, 132L);
        cuboidsWithStats.put(57088L, 499L);
        cuboidsWithStats.put(55040L, 708L);
        cuboidsWithStats.put(38656L, 36507L);

        Map<Long, Double> cuboidHitProbabilityMap = Maps.newHashMap();
        cuboidHitProbabilityMap.put(65280L, 0.2);
        cuboidHitProbabilityMap.put(63232L, 0.16);
        cuboidHitProbabilityMap.put(61184L, 0.16);
        cuboidHitProbabilityMap.put(57088L, 0.16);
        cuboidHitProbabilityMap.put(55040L, 0.16);
        cuboidHitProbabilityMap.put(38656L, 0.16);

        Map<Long, Long> cuboidsWithStatsExpected = Maps.newHashMap(cuboidsWithStats);
        cuboidsWithStatsExpected.put(65280L, 2584421L);
        cuboidsWithStatsExpected.put(57088L, 36507L);
        cuboidsWithStatsExpected.put(55040L, 36507L);
        cuboidsWithStatsExpected.put(61184L, 1212L);

        CuboidStatsUtil.adjustCuboidStats(cuboidsWithStats, statistics);
        Assert.assertEquals(cuboidsWithStatsExpected, cuboidsWithStats);
    }

    /**
     *                   1111(70)                                           1111(90)
     *                 /         \                                        /         \
     *             1011(90)       \                                    1111(90)      \
     *               |             \                                     |            \
     *             0011(40)     1110(50)          ==========>          0011(80)     1110(80)
     *            /       \   /        \                              /       \   /        \
     *        0001(20)    0010(80)    0100(60)                    0001(20)    0010(80)    0100(60)
     *
     *
     *                                                                         +
     *
     *
     *                                                                     /      \
     *                                                                    /        \
     *                                                                   /        1001(85)
     *                                                                  /           \
     *                                                                0111(70)     1000(100)
     * */
    @Test
    public void testAdjustCuboidStats() {
        Map<Long, Long> statistics = Maps.newHashMap();
        statistics.put(1L, 20L);
        statistics.put(2L, 80L);
        statistics.put(4L, 60L);
        statistics.put(3L, 40L);
        statistics.put(11L, 90L);
        statistics.put(14L, 50L);
        statistics.put(15L, 70L);

        Map<Long, Long> cuboidsWithStatsExpected = Maps.newHashMap(statistics);
        cuboidsWithStatsExpected.put(3L, 80L);
        cuboidsWithStatsExpected.put(14L, 80L);
        cuboidsWithStatsExpected.put(15L, 90L);

        statistics = CuboidStatsUtil.adjustCuboidStats(statistics);
        Assert.assertEquals(cuboidsWithStatsExpected, statistics);

        Map<Long, Long> mandatoryCuboidsWithStats = Maps.newHashMap();
        mandatoryCuboidsWithStats.put(7L, 70L);
        mandatoryCuboidsWithStats.put(8L, 100L);
        mandatoryCuboidsWithStats.put(9L, 85L);
        CuboidStatsUtil.adjustCuboidStats(mandatoryCuboidsWithStats, statistics);

        Map<Long, Long> mandatoryCuboidsWithStatsExpected = Maps.newHashMap();
        mandatoryCuboidsWithStatsExpected.put(7L, 80L);
        mandatoryCuboidsWithStatsExpected.put(8L, 80L);
        mandatoryCuboidsWithStatsExpected.put(9L, 85L);
        Assert.assertEquals(mandatoryCuboidsWithStatsExpected, mandatoryCuboidsWithStats);
    }

    @Test
    public void createDirectChildrenCacheTest() {
        Set<Long> cuboidSet = generateCuboidSet();
        Map<Long, List<Long>> directChildrenCache = CuboidStatsUtil.createDirectChildrenCache(cuboidSet);

        Assert.assertTrue(directChildrenCache.get(255L).containsAll(Lists.newArrayList(239L, 159L, 50L)));
        Assert.assertTrue(directChildrenCache.get(159L).contains(6L));
        Assert.assertTrue(directChildrenCache.get(50L).contains(2L));
        Assert.assertTrue(directChildrenCache.get(239L).contains(199L));
        Assert.assertTrue(directChildrenCache.get(199L).contains(6L));
        Assert.assertTrue(directChildrenCache.get(6L).containsAll(Lists.newArrayList(4L, 2L)));
    }

    private Set<Long> generateMassCuboidSet() {
        Set<Long> cuboidSet = Sets.newHashSet();
        long maxCuboid = (1L << 15);
        for (long i = 1; i < maxCuboid; i++) {
            cuboidSet.add(i);
        }
        return cuboidSet;
    }

    @Test
    public void createDirectChildrenCacheStressTest() {
        Stopwatch sw = Stopwatch.createUnstarted();
        sw.start();
        Set<Long> cuboidSet = generateMassCuboidSet();
        System.out.println("Time elapsed for creating sorted cuboid list: " + sw.elapsed(MILLISECONDS));
        sw.reset();
        sw.start();
        checkDirectChildrenCacheStressTest(CuboidStatsUtil.createDirectChildrenCache(cuboidSet));
        System.out.println("Time elapsed for creating direct children cache: " + sw.elapsed(MILLISECONDS));
        sw.stop();
    }

    private void checkDirectChildrenCacheStressTest(Map<Long, List<Long>> directChildrenCache) {
        for (Map.Entry<Long, List<Long>> entry : directChildrenCache.entrySet()) {
            if (Long.bitCount(entry.getKey()) == 1) {
                Assert.assertTrue("Check for cuboid " + entry.getKey(), entry.getValue().size() == 0);
            } else {
                Assert.assertTrue("Check for cuboid " + entry.getKey(),
                        Long.bitCount(entry.getKey()) == entry.getValue().size());
            }
        }
    }
}
