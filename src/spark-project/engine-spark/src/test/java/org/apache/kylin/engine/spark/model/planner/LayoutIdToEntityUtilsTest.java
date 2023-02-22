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

import org.apache.kylin.metadata.cube.planner.CostBasePlannerUtils;
import org.junit.Test;

import com.google.common.collect.Lists;

public class LayoutIdToEntityUtilsTest {

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
    public void testConvertLayoutIdsToColOrders() {
        int maxCountDimension = 12;
        Map<Long, Long> layouts = new HashMap<>();
        Map<BigInteger, Long> bigIntegerLayoutIds = new HashMap<>();
        long layout = 1 | 1 << 3 | 1 << 7;
        layouts.put(layout, (long) 0);
        bigIntegerLayoutIds.put(BigInteger.valueOf(layout), (long) 0);
        layout = 1 << 4 | 1 << 10 | 1 << 5;
        layouts.put(layout, (long) 0);
        bigIntegerLayoutIds.put(BigInteger.valueOf(layout), (long) 0);
        List<Integer> measureIds = new ArrayList<>();
        measureIds.add(1001);
        measureIds.add(1002);
        measureIds.add(1003);

        Set<List<Integer>> result = LayoutIdToEntityUtils.convertLayoutIdsToColOrders(bigIntegerLayoutIds,
                maxCountDimension, measureIds, getMap(maxCountDimension), getSortList(maxCountDimension));
        Set<List<Integer>> expected = new LinkedHashSet<>();
        expected.add(Lists.newArrayList(11 - 7, 11 - 3, 11, 1001, 1002, 1003));
        expected.add(Lists.newArrayList(11 - 10, 11 - 5, 11 - 4, 1001, 1002, 1003));
        assertEquals(expected, result);
    }

    @Test
    public void testConvertLayoutIdsToColOrdersWithReverseOrder() {
        int maxCountDimension = 12;
        Map<BigInteger, Long> layouts = new HashMap<>();
        long layout = 1 | 1 << 3 | 1 << 7;
        BigInteger bigIntegerLayoutId = BigInteger.valueOf(layout);
        layouts.put(bigIntegerLayoutId, (long) 0);
        layout = 1 << 4 | 1 << 10 | 1 << 5;
        bigIntegerLayoutId = BigInteger.valueOf(layout);
        layouts.put(bigIntegerLayoutId, (long) 0);
        List<Integer> measureIds = new ArrayList<>();
        measureIds.add(1001);
        measureIds.add(1002);
        measureIds.add(1003);
        List<Integer> sortOrder = getSortList(maxCountDimension);
        Collections.reverse(sortOrder);
        Set<List<Integer>> result = LayoutIdToEntityUtils.convertLayoutIdsToColOrders(layouts, maxCountDimension,
                measureIds, getMap(maxCountDimension), sortOrder);
        Set<List<Integer>> expected = new LinkedHashSet<>();
        expected.add(Lists.newArrayList(11, 11 - 3, 11 - 7, 1001, 1002, 1003));
        expected.add(Lists.newArrayList(11 - 4, 11 - 5, 11 - 10, 1001, 1002, 1003));
        assertEquals(expected, result);
    }

    @Test
    public void testConvertLayoutIdsToColOrdersWithEmptyOrder() {
        int maxCountDimension = 12;
        Map<BigInteger, Long> layouts = new HashMap<>();
        long layout = 1 | 1 << 3 | 1 << 7;
        BigInteger bigIntegerLayoutId = BigInteger.valueOf(layout);
        layouts.put(bigIntegerLayoutId, (long) 0);
        layout = 1 << 4 | 1 << 10 | 1 << 5;
        bigIntegerLayoutId = BigInteger.valueOf(layout);
        layouts.put(bigIntegerLayoutId, (long) 0);
        List<Integer> measureIds = new ArrayList<>();
        measureIds.add(1001);
        measureIds.add(1002);
        measureIds.add(1003);
        List<Integer> sortOrder = getSortList(maxCountDimension - 8);
        Collections.reverse(sortOrder);
        Set<List<Integer>> result = LayoutIdToEntityUtils.convertLayoutIdsToColOrders(layouts, maxCountDimension,
                measureIds, getMap(maxCountDimension), sortOrder);
        Set<List<Integer>> expected = new LinkedHashSet<>();
        assertEquals(expected, result);
    }

    @Test
    public void testConvertLayoutIdsToColOrdersWithMultiOrder() {
        int maxCountDimension = 12;
        Map<BigInteger, Long> layouts = new HashMap<>();
        long layout = 1 | 1 << 3 | 1 << 7;
        BigInteger bigIntegerLayoutId = BigInteger.valueOf(layout);
        layouts.put(bigIntegerLayoutId, (long) 0);
        layout = 1 << 4 | 1 << 10 | 1 << 5;
        bigIntegerLayoutId = BigInteger.valueOf(layout);
        layouts.put(bigIntegerLayoutId, (long) 0);
        List<Integer> measureIds = new ArrayList<>();
        measureIds.add(1001);
        measureIds.add(1002);
        measureIds.add(1003);
        List<Integer> sortOrder = getSortList(maxCountDimension);
        // order1: 0,1,2,3,4,5,6,7,8,9,10,11
        Set<List<Integer>> result = LayoutIdToEntityUtils.convertLayoutIdsToColOrders(layouts, maxCountDimension,
                measureIds, getMap(maxCountDimension), sortOrder);
        Set<List<Integer>> expected = new LinkedHashSet<>();
        expected.add(Lists.newArrayList(11 - 7, 11 - 3, 11, 1001, 1002, 1003)); // 4,8,11
        expected.add(Lists.newArrayList(11 - 10, 11 - 5, 11 - 4, 1001, 1002, 1003)); // 1,6,7
        // order2: 11,10,9,8,7,6,5,4,3,2,1,0
        Collections.reverse(sortOrder);
        result.addAll(LayoutIdToEntityUtils.convertLayoutIdsToColOrders(layouts, maxCountDimension, measureIds,
                getMap(maxCountDimension), sortOrder));
        expected.add(Lists.newArrayList(11, 11 - 3, 11 - 7, 1001, 1002, 1003)); // 11,8,4
        expected.add(Lists.newArrayList(11 - 4, 11 - 5, 11 - 10, 1001, 1002, 1003)); // 7,6,1
        assertEquals(expected, result);
    }

    @Test
    public void randomTestForDimensionWithLayout() {
        int maxCountDimension = 63;
        Random random = new Random();
        for (int i = 0; i < 100; i++) {
            long layoutId = random.nextLong();
            if (layoutId < 0) {
                layoutId = Math.abs(layoutId);
            }
            BigInteger bigIntegerLayoutId = BigInteger.valueOf(layoutId);
            List<Integer> dimensionIds = LayoutIdToEntityUtils.converLayoutToDimensionColOrder(bigIntegerLayoutId,
                    maxCountDimension, getMap(maxCountDimension), getSortList(maxCountDimension));
            BigInteger expected = CostBasePlannerUtils.convertDimensionsToLayoutId(dimensionIds, maxCountDimension,
                    getMap(maxCountDimension));
            assertEquals(expected, BigInteger.valueOf(layoutId));
        }
    }

    @Test
    public void testConvertLongToOrder() {
        // max id = 11
        int maxCountDimension = 12;
        long layout = 1 | 1 << 3 | 1 << 7;
        BigInteger bigIntegerLayoutId = BigInteger.valueOf(layout);
        List<Integer> result = LayoutIdToEntityUtils.converLayoutToDimensionColOrder(bigIntegerLayoutId,
                maxCountDimension, getMap(maxCountDimension), getSortList(maxCountDimension));
        List<Integer> expected = Lists.newArrayList(11 - 7, 11 - 3, 11);
        assertEquals(expected, result);

        layout = 1 << 4 | 1 << 10 | 1 << 5;
        bigIntegerLayoutId = BigInteger.valueOf(layout);
        result = LayoutIdToEntityUtils.converLayoutToDimensionColOrder(bigIntegerLayoutId, maxCountDimension,
                getMap(maxCountDimension), getSortList(maxCountDimension));
        expected = Lists.newArrayList(11 - 10, 11 - 5, 11 - 4);
        assertEquals(expected, result);
    }

    @Test
    public void testMore64DimensionCase() {
        int maxCountDimension = 100;
        BigInteger layout = BigInteger.ZERO;
        layout = layout.setBit(0);
        layout = layout.setBit(3);
        layout = layout.setBit(7);
        layout = layout.setBit(77);
        List<Integer> result = LayoutIdToEntityUtils.converLayoutToDimensionColOrder(layout, maxCountDimension,
                getMap(maxCountDimension), getSortList(maxCountDimension));
        List<Integer> expected = Lists.newArrayList(99 - 77, 99 - 7, 99 - 3, 99);
        assertEquals(expected, result);

        layout = BigInteger.ZERO;
        layout = layout.setBit(4);
        layout = layout.setBit(10);
        layout = layout.setBit(5);
        layout = layout.setBit(88);
        result = LayoutIdToEntityUtils.converLayoutToDimensionColOrder(layout, maxCountDimension,
                getMap(maxCountDimension), getSortList(maxCountDimension));
        expected = Lists.newArrayList(99 - 88, 99 - 10, 99 - 5, 99 - 4);
        assertEquals(expected, result);
    }
}