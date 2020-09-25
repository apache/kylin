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

import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import org.apache.kylin.shaded.com.google.common.base.Stopwatch;
import org.apache.kylin.shaded.com.google.common.collect.Sets;

public class CuboidUtilTest {
    @Test
    public void testGetLongestDepth() {
        Stopwatch sw = Stopwatch.createUnstarted();

        Set<Long> cuboidSet1 = Sets.newHashSet(7L, 6L, 5L, 4L, 3L, 2L, 1L);
        sw.start();
        assertEquals(2, CuboidUtil.getLongestDepth(cuboidSet1));
        System.out.println("Time cost for GetLongestDepth: " + sw.elapsed(TimeUnit.MILLISECONDS) + "ms");

        Set<Long> cuboidSet2 = Sets.newHashSet(1024L, 1666L, 1667L, 1728L, 1730L, 1731L, 1760L, 1762L, 1763L, 1776L,
                1778L, 1779L, 1784L, 1788L, 1790L, 1791L, 1920L, 1922L, 1923L, 1984L, 1986L, 1987L, 2016L, 2018L, 2019L,
                2032L, 2034L, 2035L, 2040L, 2044L, 2046L, 2047L);
        sw.reset();
        sw.start();
        assertEquals(8, CuboidUtil.getLongestDepth(cuboidSet2));
        System.out.println("Time cost for GetLongestDepth: " + sw.elapsed(TimeUnit.MILLISECONDS) + "ms");

        Set<Long> cuboidSet3 = Sets.newHashSet(31L, 11L, 5L, 3L, 1L);
        sw.reset();
        sw.start();
        assertEquals(3, CuboidUtil.getLongestDepth(cuboidSet3));
        System.out.println("Time cost for GetLongestDepth: " + sw.elapsed(TimeUnit.MILLISECONDS) + "ms");

        sw.stop();
    }
}
