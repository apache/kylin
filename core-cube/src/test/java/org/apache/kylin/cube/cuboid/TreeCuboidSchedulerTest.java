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

import static org.junit.Assert.assertEquals;

import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.kylin.cube.cuboid.TreeCuboidScheduler.CuboidTree;
import org.junit.Test;

import org.apache.kylin.shaded.com.google.common.collect.Maps;

public class TreeCuboidSchedulerTest {

    @Test
    public void testCreateCuboidTree() {
        long basicCuboid = getBaseCuboid(10);
        List<Long> cuboids = genRandomCuboids(basicCuboid, 200);
        CuboidTree cuboidTree = CuboidTree.createFromCuboids(cuboids);
        PrintWriter out = new PrintWriter(new OutputStreamWriter(System.out, StandardCharsets.UTF_8));
        cuboidTree.print(out);
        out.flush();
    }

    @Test
    public void testSpanningChild() {
        long basicCuboid = getBaseCuboid(10);
        List<Long> cuboids = genRandomCuboids(basicCuboid, 50);
        long testCuboid = cuboids.get(10);
        System.out.println(cuboids);
        CuboidTree cuboidTree = CuboidTree.createFromCuboids(cuboids);
        PrintWriter out = new PrintWriter(new OutputStreamWriter(System.out, StandardCharsets.UTF_8));
        cuboidTree.print(out);
        out.flush();

        List<Long> spanningChildren = cuboidTree.getSpanningCuboid(testCuboid);
        System.out.println(testCuboid + ":" + spanningChildren);
    }

    @Test
    public void testFindBestMatchCuboid() {
        CuboidTree cuboidTree = createCuboidTree1();
        PrintWriter out = new PrintWriter(new OutputStreamWriter(System.out, StandardCharsets.UTF_8));
        cuboidTree.print(out);
        out.flush();

        assertEquals(503L, cuboidTree.findBestMatchCuboid(503L));

        long bestMatch1 = cuboidTree.findBestMatchCuboid(Long.parseLong("100000000", 2));
        assertEquals(263, bestMatch1);

        long bestMatch2 = cuboidTree.findBestMatchCuboid(Long.parseLong("100010000", 2));
        assertEquals(304, bestMatch2);
    }

    private List<Long> genRandomCuboids(long basicCuboidId, int count) {
        Random random = new Random();
        List<Long> result = new ArrayList<>();
        result.add(basicCuboidId);
        for (int i = 0; i < count; i++) {
            result.add(random.nextLong() & basicCuboidId);
        }
        return result;
    }

    private long getBaseCuboid(int dimensionCnt) {
        if (dimensionCnt > 64) {
            throw new IllegalArgumentException("the dimension count cannot exceed 64");
        }
        long result = 0;
        for (int i = 0; i < dimensionCnt; i++) {
            result |= (1 << i);
        }
        return result;
    }

    private CuboidTree createCuboidTree1() {
        List<Long> cuboids = Arrays.asList(504L, 511L, 447L, 383L, 503L, 440L, 496L, 376L, 439L, 487L, 375L, 319L, 432L,
                480L, 368L, 312L, 423L, 455L, 311L, 359L, 416L, 448L, 304L, 352L, 391L, 295L, 327L, 384L, 288L, 320L,
                263L);
        return CuboidTree.createFromCuboids(cuboids,
                new TreeCuboidScheduler.CuboidCostComparator(simulateStatistics()));
    }

    private Map<Long, Long> simulateStatistics() {
        Map<Long, Long> countMap = Maps.newHashMap();
        countMap.put(511L, 1000000L);

        countMap.put(504L, 900000L);
        countMap.put(447L, 990000L);
        countMap.put(383L, 991000L);
        countMap.put(503L, 980000L);

        countMap.put(440L, 800000L);
        countMap.put(496L, 890000L);
        countMap.put(376L, 891000L);
        countMap.put(439L, 751000L);
        countMap.put(487L, 751000L);
        countMap.put(375L, 741000L);
        countMap.put(319L, 740000L);

        countMap.put(432L, 600000L);
        countMap.put(480L, 690000L);
        countMap.put(368L, 691000L);
        countMap.put(312L, 651000L);
        countMap.put(423L, 651000L);
        countMap.put(455L, 541000L);
        countMap.put(311L, 540000L);
        countMap.put(359L, 530000L);

        countMap.put(416L, 400000L);
        countMap.put(448L, 490000L);
        countMap.put(304L, 491000L);
        countMap.put(352L, 451000L);
        countMap.put(391L, 351000L);
        countMap.put(295L, 141000L);
        countMap.put(327L, 240000L);

        countMap.put(384L, 100000L);
        countMap.put(288L, 90000L);
        countMap.put(320L, 91000L);
        countMap.put(263L, 51000L);

        return countMap;
    }
}
