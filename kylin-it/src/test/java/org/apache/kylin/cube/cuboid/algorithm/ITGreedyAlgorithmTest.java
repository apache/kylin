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

import java.util.List;

import org.apache.kylin.cube.cuboid.algorithm.greedy.GreedyAlgorithm;
import org.junit.Test;

public class ITGreedyAlgorithmTest extends ITAlgorithmTestBase {

    @Test
    public void testBPUSCalculator() {
        BenefitPolicy benefitPolicy = new BPUSCalculator(cuboidStats);
        GreedyAlgorithm algorithm = new GreedyAlgorithm(-1, benefitPolicy, cuboidStats);

        List<Long> recommendList = algorithm.recommend(10);
        System.out.println("recommendList by BPUSCalculator: " + recommendList);
        System.out.println("Cost evaluated for each query: " + getQueryCostRatio(cuboidStats, recommendList));
    }

    @Test
    public void testPBPUSCalculator() {
        BenefitPolicy benefitPolicy = new PBPUSCalculator(cuboidStats);
        GreedyAlgorithm algorithm = new GreedyAlgorithm(-1, benefitPolicy, cuboidStats);

        List<Long> recommendList = algorithm.recommend(10);
        System.out.println("recommendList by PBPUSCalculator:" + recommendList);
        System.out.println("Cost evaluated for each query: " + getQueryCostRatio(cuboidStats, recommendList));
    }

    @Test
    public void testSPBPUSCalculator() {
        BenefitPolicy benefitPolicy = new SPBPUSCalculator(cuboidStats);
        GreedyAlgorithm algorithm = new GreedyAlgorithm(-1, benefitPolicy, cuboidStats);

        List<Long> recommendList = algorithm.recommend(10);
        System.out.println("recommendList by SPBPUSCalculator:" + recommendList);
        System.out.println("Cost evaluated for each query: " + getQueryCostRatio(cuboidStats, recommendList));
    }

}
