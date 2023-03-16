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
package org.apache.kylin.metadata.cube.planner.algorithm.greedy;

import java.math.BigInteger;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.metadata.cube.planner.algorithm.AlgorithmTestBase;
import org.apache.kylin.metadata.cube.planner.algorithm.BPUSCalculator;
import org.apache.kylin.metadata.cube.planner.algorithm.BenefitPolicy;
import org.apache.kylin.metadata.cube.planner.algorithm.PBPUSCalculator;
import org.apache.kylin.metadata.cube.planner.algorithm.SPBPUSCalculator;
import org.junit.Assert;
import org.junit.Test;

public class GreedyAlgorithmTest extends AlgorithmTestBase {
    @Test
    public void testBPUSCalculator() {
        BenefitPolicy benefitPolicy = new BPUSCalculator(cuboidStats);
        GreedyAlgorithm algorithm = new GreedyAlgorithm(-1, benefitPolicy, cuboidStats);

        List<BigInteger> recommendList = algorithm.recommend(10);
        // The result is copy from the result of 3.1 test
        List<Integer> expected = Lists.newArrayList(1888, 3000, 88, 4095, 258, 1152, 1456, 274, 448, 34, 386, 336, 1528,
                290, 322, 434, 1410, 1514, 338, 1976, 1458, 984, 1530, 770, 2040, 1962, 786, 890, 1978, 257, 260, 1994,
                2042, 273, 289, 276, 385, 292, 321, 388, 1513, 324, 259, 2368, 305, 262, 1532, 401, 337, 2306, 1529,
                3560, 275, 2384, 1531, 278, 3576, 2041, 1534, 2322, 1980, 3578, 4088, 2046, 2043, 3582, 2047, 4090,
                2352, 2448, 291, 876, 422, 352, 2338, 387, 769, 308, 323, 2434, 404, 3583, 326, 2370, 1961, 2044, 1444,
                4092, 435, 2482, 1664, 2037, 340, 3976, 492, 800, 3498, 1451, 3572, 438, 1454, 496, 889, 2920, 4081,
                872, 1457, 811, 1256, 370, 3504, 2858, 48, 466, 814, 371, 2418, 374, 4094, 1928, 400, 1963, 1992, 1460,
                988, 891, 4010, 2938);
        List<BigInteger> expectedBigInteger = expected.stream().map(new Function<Integer, BigInteger>() {
            @Override
            public BigInteger apply(Integer integer) {
                return new BigInteger(integer.toString());
            }
        }).collect(Collectors.toList());
        Assert.assertEquals(expectedBigInteger, recommendList);
        System.out.println("recommendList by BPUSCalculator: " + recommendList);

        System.out.println("Cost evaluated for each query: " + getQueryCostRatio(cuboidStats, recommendList));
    }

    @Test
    public void testPBPUSCalculator() {
        BenefitPolicy benefitPolicy = new PBPUSCalculator(cuboidStats);
        GreedyAlgorithm algorithm = new GreedyAlgorithm(-1, benefitPolicy, cuboidStats);

        List<BigInteger> recommendList = algorithm.recommend(10);
        // The result is copy from the result of 3.1 test
        List<Integer> expected = Lists.newArrayList(1888, 3000, 88, 4095, 2, 1152, 258, 274, 336, 1440, 488, 34, 432,
                1528, 386, 290, 322, 434, 1410, 1514, 338, 1976, 1458, 984, 1530, 770, 2040, 1962, 786, 890, 1978, 257,
                260, 1994, 2768, 2042, 273, 256, 289, 276, 385, 292, 352, 321, 388, 288, 1513, 324, 259, 305, 262, 1532,
                401, 272, 2306, 337, 1529, 3560, 3780, 128, 275, 1531, 278, 3576, 3145, 2041, 1534, 2322, 384, 3578,
                1980, 3459, 4013, 2043, 2046, 4088, 320, 32, 3582, 4090, 2047, 3977, 2352, 2448, 291, 876, 422, 2338,
                387, 769, 308, 323, 2434, 1168, 2384, 2861, 404, 326, 2370, 1961, 2044, 1444, 435, 3583, 2482, 1664,
                340);
        List<BigInteger> expectedBigInteger = expected.stream().map(new Function<Integer, BigInteger>() {
            @Override
            public BigInteger apply(Integer integer) {
                return new BigInteger(integer.toString());
            }
        }).collect(Collectors.toList());
        Assert.assertEquals(expectedBigInteger, recommendList);
        System.out.println("recommendList by PBPUSCalculator:" + recommendList);
        System.out.println("Cost evaluated for each query: " + getQueryCostRatio(cuboidStats, recommendList));
    }

    @Test
    public void testSPBPUSCalculator() {
        BenefitPolicy benefitPolicy = new SPBPUSCalculator(cuboidStats);
        GreedyAlgorithm algorithm = new GreedyAlgorithm(-1, benefitPolicy, cuboidStats);

        List<BigInteger> recommendList = algorithm.recommend(10);
        // The result is copy from the result of 3.1 test
        List<Integer> expected = Lists.newArrayList(1888, 3000, 88, 4095, 256, 2, 288, 128, 272, 1152, 258, 274, 336,
                384, 352, 1440, 432, 488, 34, 1528, 386, 768, 290, 322, 434, 1664, 1410, 1960, 784, 1514, 338, 1976,
                832, 1458, 1992, 1530, 888, 770, 2040, 1962, 786, 890, 1978, 257, 260, 2768, 1994, 2042, 2304, 273, 289,
                276, 385, 292, 321, 388, 2336, 1513, 324, 259, 305, 2320, 3780, 262, 401, 1532, 337, 3576, 2306, 1529,
                3145, 275, 4094, 1531, 2861, 3459, 3849, 2041, 2043, 2047, 291, 323, 3583, 435, 2037, 969, 811, 953,
                370, 4081, 466, 491, 1457, 339, 1961, 1963, 1459, 891, 497, 889, 1535, 1680, 1210, 308, 1515, 1690);
        List<BigInteger> expectedBigInteger = expected.stream().map(new Function<Integer, BigInteger>() {
            @Override
            public BigInteger apply(Integer integer) {
                return new BigInteger(integer.toString());
            }
        }).collect(Collectors.toList());
        Assert.assertEquals(expectedBigInteger, recommendList);
        System.out.println("recommendList by SPBPUSCalculator:" + recommendList);
        System.out.println("Cost evaluated for each query: " + getQueryCostRatio(cuboidStats, recommendList));
    }
}
