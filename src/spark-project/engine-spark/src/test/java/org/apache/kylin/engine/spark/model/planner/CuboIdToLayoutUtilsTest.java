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
package org.apache.kylin.engine.spark.model.planner;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.metadata.cube.planner.CostBasePlannerUtils;
import org.junit.Test;

public class CuboIdToLayoutUtilsTest {

    private Map<Integer, Integer> getMap(int dimensionCount) {
        Map<Integer, Integer> result = new HashMap<>();
        for (int i = 0; i < dimensionCount; i++) {
            result.put(i, i);
        }
        return result;
    }

    private List<Integer> getSortList(int dimensionCount) {
        List<Integer> result = new ArrayList<>();
        for (int i = 0; i < dimensionCount; i++) {
            result.add(i);
        }
        return result;
    }

    @Test
    public void testConvertCuboIdsToColOrders() {
        int maxCountDimension = 12;
        Map<Long, Long> cuboids = new HashMap<>();
        Map<BigInteger, Long> bigIntegerCuboIds = new HashMap<>();
        long cuboid = 1 << 0 | 1 << 3 | 1 << 7;
        cuboids.put(cuboid, (long) 0);
        bigIntegerCuboIds.put(BigInteger.valueOf(cuboid), (long) 0);
        cuboid = 1 << 4 | 1 << 10 | 1 << 5;
        cuboids.put(cuboid, (long) 0);
        bigIntegerCuboIds.put(BigInteger.valueOf(cuboid), (long) 0);
        List<Integer> measureIds = new ArrayList<>();
        measureIds.add(1001);
        measureIds.add(1002);
        measureIds.add(1003);

        Set<List<Integer>> result = CuboIdToLayoutUtils.convertCuboIdsToColOrders(bigIntegerCuboIds, maxCountDimension,
                measureIds, getMap(maxCountDimension), getSortList(maxCountDimension));
        Set<List<Integer>> expected = new LinkedHashSet<>();
        expected.add(Lists.newArrayList(11 - 7, 11 - 3, 11 - 0, 1001, 1002, 1003));
        expected.add(Lists.newArrayList(11 - 10, 11 - 5, 11 - 4, 1001, 1002, 1003));
        assertEquals(expected, result);
    }

    @Test
    public void testConvertCuboIdsToColOrdersWithReverseOrder() {
        int maxCountDimension = 12;
        Map<BigInteger, Long> cuboids = new HashMap<>();
        long cuboid = 1 << 0 | 1 << 3 | 1 << 7;
        BigInteger bigIntegerCuboId = BigInteger.valueOf(cuboid);
        cuboids.put(bigIntegerCuboId, (long) 0);
        cuboid = 1 << 4 | 1 << 10 | 1 << 5;
        bigIntegerCuboId = BigInteger.valueOf(cuboid);
        cuboids.put(bigIntegerCuboId, (long) 0);
        List<Integer> measureIds = new ArrayList<>();
        measureIds.add(1001);
        measureIds.add(1002);
        measureIds.add(1003);
        List<Integer> sortOrder = getSortList(maxCountDimension);
        Collections.reverse(sortOrder);
        Set<List<Integer>> result = CuboIdToLayoutUtils.convertCuboIdsToColOrders(cuboids, maxCountDimension,
                measureIds, getMap(maxCountDimension), sortOrder);
        Set<List<Integer>> expected = new LinkedHashSet<>();
        expected.add(Lists.newArrayList(11 - 0, 11 - 3, 11 - 7, 1001, 1002, 1003));
        expected.add(Lists.newArrayList(11 - 4, 11 - 5, 11 - 10, 1001, 1002, 1003));
        assertEquals(expected, result);
    }

    @Test
    public void testConvertCuboIdsToColOrdersWithEmptyOrder() {
        int maxCountDimension = 12;
        Map<BigInteger, Long> cuboids = new HashMap<>();
        long cuboid = 1 << 0 | 1 << 3 | 1 << 7;
        BigInteger bigIntegerCuboId = BigInteger.valueOf(cuboid);
        cuboids.put(bigIntegerCuboId, (long) 0);
        cuboid = 1 << 4 | 1 << 10 | 1 << 5;
        bigIntegerCuboId = BigInteger.valueOf(cuboid);
        cuboids.put(bigIntegerCuboId, (long) 0);
        List<Integer> measureIds = new ArrayList<>();
        measureIds.add(1001);
        measureIds.add(1002);
        measureIds.add(1003);
        List<Integer> sortOrder = getSortList(maxCountDimension - 8);
        Collections.reverse(sortOrder);
        Set<List<Integer>> result = CuboIdToLayoutUtils.convertCuboIdsToColOrders(cuboids, maxCountDimension,
                measureIds, getMap(maxCountDimension), sortOrder);
        Set<List<Integer>> expected = new LinkedHashSet<>();
        assertEquals(expected, result);
    }

    @Test
    public void testConvertCuboIdsToColOrdersWithMultiOrder() {
        int maxCountDimension = 12;
        Map<BigInteger, Long> cuboids = new HashMap<>();
        long cuboid = 1 << 0 | 1 << 3 | 1 << 7;
        BigInteger bigIntegerCuboId = BigInteger.valueOf(cuboid);
        cuboids.put(bigIntegerCuboId, (long) 0);
        cuboid = 1 << 4 | 1 << 10 | 1 << 5;
        bigIntegerCuboId = BigInteger.valueOf(cuboid);
        cuboids.put(bigIntegerCuboId, (long) 0);
        List<Integer> measureIds = new ArrayList<>();
        measureIds.add(1001);
        measureIds.add(1002);
        measureIds.add(1003);
        List<Integer> sortOrder = getSortList(maxCountDimension);
        // order1: 0,1,2,3,4,5,6,7,8,9,10,11
        Set<List<Integer>> result = CuboIdToLayoutUtils.convertCuboIdsToColOrders(cuboids, maxCountDimension,
                measureIds, getMap(maxCountDimension), sortOrder);
        Set<List<Integer>> expected = new LinkedHashSet<>();
        expected.add(Lists.newArrayList(11 - 7, 11 - 3, 11 - 0, 1001, 1002, 1003)); // 4,8,11
        expected.add(Lists.newArrayList(11 - 10, 11 - 5, 11 - 4, 1001, 1002, 1003)); // 1,6,7
        // order2: 11,10,9,8,7,6,5,4,3,2,1,0
        Collections.reverse(sortOrder);
        result.addAll(CuboIdToLayoutUtils.convertCuboIdsToColOrders(cuboids, maxCountDimension, measureIds,
                getMap(maxCountDimension), sortOrder));
        expected.add(Lists.newArrayList(11 - 0, 11 - 3, 11 - 7, 1001, 1002, 1003)); // 11,8,4
        expected.add(Lists.newArrayList(11 - 4, 11 - 5, 11 - 10, 1001, 1002, 1003)); // 7,6,1
        assertEquals(expected, result);
    }

    @Test
    public void randomTestForDimensionWithCuboid() {
        int maxCountDimension = 63;
        Random random = new Random();
        for (int i = 0; i < 100; i++) {
            long cuboId = random.nextLong();
            if (cuboId < 0) {
                cuboId = Math.abs(cuboId);
            }
            BigInteger bigIntegerCuboId = BigInteger.valueOf(cuboId);
            List<Integer> dimensionIds = CuboIdToLayoutUtils.converCuboidToDimensionColOrder(bigIntegerCuboId,
                    maxCountDimension, getMap(maxCountDimension), getSortList(maxCountDimension));
            BigInteger expected = CostBasePlannerUtils.convertDimensionsToCuboId(dimensionIds, maxCountDimension,
                    getMap(maxCountDimension));
            assertEquals(expected, BigInteger.valueOf(cuboId));
        }
    }

    @Test
    public void testConvertLongToOrder() {
        // max id = 11
        int maxCountDimension = 12;
        long cuboid = 1 << 0 | 1 << 3 | 1 << 7;
        BigInteger bigIntegerCuboId = BigInteger.valueOf(cuboid);
        List<Integer> result = CuboIdToLayoutUtils.converCuboidToDimensionColOrder(bigIntegerCuboId, maxCountDimension,
                getMap(maxCountDimension), getSortList(maxCountDimension));
        List<Integer> expected = Lists.newArrayList(11 - 7, 11 - 3, 11 - 0);
        assertEquals(expected, result);

        cuboid = 1 << 4 | 1 << 10 | 1 << 5;
        bigIntegerCuboId = BigInteger.valueOf(cuboid);
        result = CuboIdToLayoutUtils.converCuboidToDimensionColOrder(bigIntegerCuboId, maxCountDimension,
                getMap(maxCountDimension), getSortList(maxCountDimension));
        expected = Lists.newArrayList(11 - 10, 11 - 5, 11 - 4);
        assertEquals(expected, result);
    }

    @Test
    public void testMore64DimensionCase() {
        int maxCountDimension = 100;
        BigInteger cuboid = BigInteger.ZERO;
        cuboid = cuboid.setBit(0);
        cuboid = cuboid.setBit(3);
        cuboid = cuboid.setBit(7);
        cuboid = cuboid.setBit(77);
        List<Integer> result = CuboIdToLayoutUtils.converCuboidToDimensionColOrder(cuboid, maxCountDimension,
                getMap(maxCountDimension), getSortList(maxCountDimension));
        List<Integer> expected = Lists.newArrayList(99 - 77, 99 - 7, 99 - 3, 99 - 0);
        assertEquals(expected, result);

        cuboid = BigInteger.ZERO;
        cuboid = cuboid.setBit(4);
        cuboid = cuboid.setBit(10);
        cuboid = cuboid.setBit(5);
        cuboid = cuboid.setBit(88);
        result = CuboIdToLayoutUtils.converCuboidToDimensionColOrder(cuboid, maxCountDimension,
                getMap(maxCountDimension), getSortList(maxCountDimension));
        expected = Lists.newArrayList(99 - 88, 99 - 10, 99 - 5, 99 - 4);
        assertEquals(expected, result);
    }
}
