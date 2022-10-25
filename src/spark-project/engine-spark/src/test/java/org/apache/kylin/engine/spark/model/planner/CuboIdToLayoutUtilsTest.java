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

import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Test;

import com.google.common.collect.Lists;

public class CuboIdToLayoutUtilsTest {


    @Test
    public void testConvertLongToOrder() {
        // max id = 11
        int maxCountDimension = 12;
        long cuboid = 1 << 0 | 1 << 3 | 1 << 7;
        List<Integer> result = CuboIdToLayoutUtils.convertLongToDimensionColOrder(cuboid, maxCountDimension);
        List<Integer> expected = Lists.newArrayList(11 - 7, 11 - 3, 11 - 0);
        assertEquals(expected, result);


        cuboid = 1 << 4 | 1 << 10 | 1 << 5;
        result = CuboIdToLayoutUtils.convertLongToDimensionColOrder(cuboid, maxCountDimension);
        expected = Lists.newArrayList(11 - 10, 11 - 5, 11 - 4);
        assertEquals(expected, result);
    }

    @Test
    public void convertCuboIdsToColOrders() {
        int maxCountDimension = 12;
        Map<Long, Long> cuboids = new HashMap<>();
        long cuboid = 1 << 0 | 1 << 3 | 1 << 7;
        cuboids.put(cuboid, (long) 0);
        cuboid = 1 << 4 | 1 << 10 | 1 << 5;
        cuboids.put(cuboid, (long) 0);
        Set<Integer> measureIds = new LinkedHashSet<>();
        measureIds.add(1001);
        measureIds.add(1002);
        measureIds.add(1003);
        Set<List<Integer>> result = CuboIdToLayoutUtils.convertCuboIdsToColOrders(cuboids, maxCountDimension, measureIds);
        Set<List<Integer>> expected = new LinkedHashSet<>();
        expected.add(Lists.newArrayList(11 - 7, 11 - 3, 11 - 0, 1001, 1002, 1003));
        expected.add(Lists.newArrayList(11 - 10, 11 - 5, 11 - 4, 1001, 1002, 1003));
        assertEquals(expected, result);
    }
}