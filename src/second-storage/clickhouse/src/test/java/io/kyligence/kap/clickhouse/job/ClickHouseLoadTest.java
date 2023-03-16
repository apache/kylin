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
package io.kyligence.kap.clickhouse.job;

import org.apache.kylin.guava30.shaded.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;

public class ClickHouseLoadTest {

    @Test
    public void getSizeOrderShardsTest() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        ClickHouseLoad clickHouseLoad = new ClickHouseLoad();

        Method getIndexInGroup = clickHouseLoad.getClass().getDeclaredMethod("getIndexInGroup", String[].class, Map.class);
        getIndexInGroup.setAccessible(true);
        Method orderGroupByIndex = clickHouseLoad.getClass().getDeclaredMethod("orderGroupByIndex", String[].class, int[].class);
        orderGroupByIndex.setAccessible(true);

        String node11 = "node11";
        String node21 = "node21";
        String node31 = "node31";

        int[] result1 = (int[]) getIndexInGroup.invoke(
                clickHouseLoad,
                new String[]{node11, node21, node31},
                ImmutableMap.of(node11, 10L, node21, 15L, node31, 16L));
        String[] result11 = (String[]) orderGroupByIndex.invoke(
                clickHouseLoad,
                new String[]{node11, node21, node31},
                result1);
        Assert.assertArrayEquals(result1, new int[]{0, 1, 2});
        Assert.assertArrayEquals(result11, new String[]{node11, node21, node31});

        int[] result2 = (int[]) getIndexInGroup.invoke(
                clickHouseLoad,
                new String[]{node11, node21, node31},
                ImmutableMap.of(node11, 16L, node21, 15L, node31, 17L));
        String[] result22 = (String[]) orderGroupByIndex.invoke(
                clickHouseLoad,
                new String[]{node11, node21, node31},
                result2);
        Assert.assertArrayEquals(result2, new int[]{1, 0, 2});
        Assert.assertArrayEquals(result22, new String[]{node21, node11, node31});

        int[] result3 = (int[]) getIndexInGroup.invoke(
                clickHouseLoad,
                new String[]{node11, node21, node31},
                ImmutableMap.of(node11, 18L, node21, 15L, node31, 10L));
        String[] result33 = (String[]) orderGroupByIndex.invoke(
                clickHouseLoad,
                new String[]{node11, node21, node31},
                result3);
        Assert.assertArrayEquals(result3, new int[]{2, 1, 0});
        Assert.assertArrayEquals(result33, new String[]{node31, node21, node11});
    }
}
