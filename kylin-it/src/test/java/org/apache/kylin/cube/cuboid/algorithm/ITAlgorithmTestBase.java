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

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kylin.common.util.StringUtil;
import org.apache.kylin.cube.cuboid.TreeCuboidScheduler.CuboidCostComparator;
import org.apache.kylin.cube.cuboid.TreeCuboidScheduler.CuboidTree;
import org.junit.After;
import org.junit.Before;

import org.apache.kylin.shaded.com.google.common.collect.Maps;
import org.apache.kylin.shaded.com.google.common.collect.Sets;

public class ITAlgorithmTestBase {

    public CuboidStats cuboidStats;

    private Set<Long> mandatoryCuboids;

    @Before
    public void setUp() throws Exception {

        mandatoryCuboids = Sets.newHashSet();
        mandatoryCuboids.add(3000L);
        mandatoryCuboids.add(1888L);
        mandatoryCuboids.add(88L);
        cuboidStats = new CuboidStats.Builder("test", 4095L, simulateCount(), simulateSpaceSize())
                .setMandatoryCuboids(mandatoryCuboids).setHitFrequencyMap(simulateHitFrequency())
                .setScanCountSourceMap(simulateScanCount()).build();
    }

    @After
    public void after() throws Exception {
    }

    /** better if closer to 1, worse if closer to 0*/
    public double getQueryCostRatio(CuboidStats cuboidStats, List<Long> recommendList) {
        CuboidTree cuboidTree = CuboidTree.createFromCuboids(recommendList,
                new CuboidCostComparator(cuboidStats.getStatistics()));
        double queryCostBest = 0;
        for (Long cuboidId : cuboidStats.getAllCuboidsForSelection()) {
            if (cuboidStats.getCuboidQueryCost(cuboidId) != null) {
                queryCostBest += cuboidStats.getCuboidHitProbability(cuboidId) * cuboidStats.getCuboidCount(cuboidId);
                //                queryCostBest += cuboidStats.getCuboidHitProbability(cuboidId) * cuboidStats.getCuboidQueryCost(cuboidId);
            }
        }

        double queryCost = 0;
        for (Long cuboidId : cuboidStats.getAllCuboidsForSelection()) {
            long matchCuboidId = cuboidTree.findBestMatchCuboid(cuboidId);
            if (cuboidStats.getCuboidQueryCost(matchCuboidId) != null) {
                queryCost += cuboidStats.getCuboidHitProbability(cuboidId) * cuboidStats.getCuboidCount(matchCuboidId);
                //                queryCost += cuboidStats.getCuboidHitProbability(cuboidId) * cuboidStats.getCuboidQueryCost(matchCuboidId);
            }
        }

        return queryCostBest / queryCost;
    }

    protected Map<Long, Long> simulateCount() {
        Map<Long, Long> countMap = Maps.newHashMap();
        BufferedReader br = null;

        try {

            String sCurrentLine;

            br = new BufferedReader(new InputStreamReader(new FileInputStream("src/test/resources/statistics.txt"),
                    StandardCharsets.UTF_8));

            while ((sCurrentLine = br.readLine()) != null) {
                String[] statPair = StringUtil.split(sCurrentLine, " ");
                countMap.put(Long.valueOf(statPair[0]), Long.valueOf(statPair[1]));
            }

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (br != null)
                    br.close();
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }

        return countMap;
    }

    protected Map<Long, Double> simulateSpaceSize() {
        Map<Long, Double> sizeMap = Maps.newHashMap();
        Map<Long, Long> countMap = simulateCount();
        for (Map.Entry<Long, Long> entry : countMap.entrySet()) {
            sizeMap.put(entry.getKey(), entry.getValue() * 1.0);
        }
        return sizeMap;
    }

    protected Map<Long, Long> simulateHitFrequency() {
        Map<Long, Long> hitFrequencyMap = Maps.newHashMap();

        hitFrequencyMap.put(4095L, 10L);
        hitFrequencyMap.put(3849L, 15L);
        hitFrequencyMap.put(3780L, 31L);

        hitFrequencyMap.put(3459L, 16L);
        hitFrequencyMap.put(3145L, 29L);

        hitFrequencyMap.put(2861L, 21L);
        hitFrequencyMap.put(2768L, 40L);

        hitFrequencyMap.put(1528L, 10L);
        hitFrequencyMap.put(1440L, 9L);
        hitFrequencyMap.put(1152L, 21L);

        hitFrequencyMap.put(256L, 23L);

        hitFrequencyMap.put(128L, 7L);
        hitFrequencyMap.put(272L, 8L);
        hitFrequencyMap.put(288L, 10L);
        hitFrequencyMap.put(384L, 2L);
        hitFrequencyMap.put(320L, 3L);
        hitFrequencyMap.put(432L, 5L);
        hitFrequencyMap.put(258L, 8L);
        hitFrequencyMap.put(336L, 10L);
        hitFrequencyMap.put(274L, 22L);
        hitFrequencyMap.put(488L, 41L);
        hitFrequencyMap.put(352L, 10L);

        hitFrequencyMap.put(16L, 1L);
        hitFrequencyMap.put(32L, 5L);
        hitFrequencyMap.put(34L, 1L);

        hitFrequencyMap.put(2L, 21L);

        return hitFrequencyMap;
    }

    protected Map<Long, Map<Long, Long>> simulateScanCount() {
        Map<Long, Map<Long, Long>> scanCountMap = Maps.newLinkedHashMap();
        scanCountMap.put(4094L, new HashMap<Long, Long>() {
            {
                put(4095L, 1833041L);
            }
        });
        scanCountMap.put(3849L, new HashMap<Long, Long>() {
            {
                put(3849L, 276711L);
            }
        });
        scanCountMap.put(3780L, new HashMap<Long, Long>() {
            {
                put(3780L, 129199L);
            }
        });
        scanCountMap.put(3459L, new HashMap<Long, Long>() {
            {
                put(3459L, 168109L);
            }
        });
        scanCountMap.put(3145L, new HashMap<Long, Long>() {
            {
                put(3145L, 299991L);
            }
        });
        scanCountMap.put(2861L, new HashMap<Long, Long>() {
            {
                put(2861L, 2121L);
            }
        });
        scanCountMap.put(2768L, new HashMap<Long, Long>() {
            {
                put(2768L, 40231L);
            }
        });
        scanCountMap.put(256L, new HashMap<Long, Long>() {
            {
                put(256L, 1L);
            }
        });
        scanCountMap.put(16L, new HashMap<Long, Long>() {
            {
                put(16L, 1L);
            }
        });
        scanCountMap.put(32L, new HashMap<Long, Long>() {
            {
                put(32L, 2L);
            }
        });
        scanCountMap.put(128L, new HashMap<Long, Long>() {
            {
                put(128L, 3L);
            }
        });
        scanCountMap.put(272L, new HashMap<Long, Long>() {
            {
                put(272L, 2L);
            }
        });
        scanCountMap.put(288L, new HashMap<Long, Long>() {
            {
                put(288L, 3L);
            }
        });
        scanCountMap.put(2L, new HashMap<Long, Long>() {
            {
                put(2L, 1L);
            }
        });
        scanCountMap.put(384L, new HashMap<Long, Long>() {
            {
                put(384L, 2L);
            }
        });
        scanCountMap.put(320L, new HashMap<Long, Long>() {
            {
                put(320L, 3L);
            }
        });
        scanCountMap.put(432L, new HashMap<Long, Long>() {
            {
                put(432L, 5L);
            }
        });
        scanCountMap.put(1152L, new HashMap<Long, Long>() {
            {
                put(1152L, 21L);
            }
        });
        scanCountMap.put(258L, new HashMap<Long, Long>() {
            {
                put(258L, 2L);
            }
        });
        scanCountMap.put(1440L, new HashMap<Long, Long>() {
            {
                put(1440L, 9L);
            }
        });
        scanCountMap.put(336L, new HashMap<Long, Long>() {
            {
                put(336L, 2L);
            }
        });
        scanCountMap.put(336L, new HashMap<Long, Long>() {
            {
                put(336L, 2L);
            }
        });
        scanCountMap.put(274L, new HashMap<Long, Long>() {
            {
                put(274L, 1L);
            }
        });
        scanCountMap.put(488L, new HashMap<Long, Long>() {
            {
                put(488L, 16L);
            }
        });
        scanCountMap.put(352L, new HashMap<Long, Long>() {
            {
                put(352L, 3L);
            }
        });
        scanCountMap.put(1528L, new HashMap<Long, Long>() {
            {
                put(1528L, 21L);
            }
        });
        scanCountMap.put(34L, new HashMap<Long, Long>() {
            {
                put(34L, 1L);
            }
        });

        return scanCountMap;
    }
}
