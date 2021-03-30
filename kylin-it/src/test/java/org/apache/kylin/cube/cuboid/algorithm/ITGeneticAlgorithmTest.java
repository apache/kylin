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

import org.apache.kylin.shaded.com.google.common.collect.Sets;
import org.apache.commons.math3.genetics.Chromosome;
import org.apache.kylin.cube.cuboid.algorithm.generic.BitsChromosome;
import org.apache.kylin.cube.cuboid.algorithm.generic.BitsChromosomeHelper;
import org.apache.kylin.cube.cuboid.algorithm.generic.GeneticAlgorithm;
import org.junit.Test;

import java.util.BitSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;

//@Ignore("testBPUSCalculator() is unsable; whole test takes too long")
public class ITGeneticAlgorithmTest extends ITAlgorithmTestBase {

    @Test
    public void testChromosomeIsSame() {
        BenefitPolicy benefitPolicy = new BPUSCalculator(cuboidStats);

        double maxSpaceLimit = cuboidStats.getBaseCuboidSize() * 10;
        BitsChromosomeHelper helper = new BitsChromosomeHelper(maxSpaceLimit, cuboidStats);

        double maxSpaceLimit1 = cuboidStats.getBaseCuboidSize() * 12;
        BitsChromosomeHelper helper1 = new BitsChromosomeHelper(maxSpaceLimit1, cuboidStats);

        BitSet representation = new BitSet();
        representation.set(10);
        Chromosome chromosome = new BitsChromosome(representation, benefitPolicy, helper);
        Set<Chromosome> chromosomeSet = Sets.newHashSet(chromosome);

        BitSet representation1 = new BitSet();
        representation1.set(10);
        chromosomeSet.add(((BitsChromosome) chromosome).newBitsChromosome(representation1));
        assertEquals(1, chromosomeSet.size());

        BitSet representation2 = new BitSet();
        representation2.set(12);
        chromosomeSet.add(((BitsChromosome) chromosome).newBitsChromosome(representation2));
        assertEquals(2, chromosomeSet.size());

        BitSet representation3 = new BitSet();
        representation3.set(12);
        chromosomeSet.add(new BitsChromosome(representation3, benefitPolicy, helper1));
        assertEquals(2, chromosomeSet.size());
    }

    @Test
    public void testBPUSCalculator() {
        BenefitPolicy benefitPolicy = new BPUSCalculator(cuboidStats);
        GeneticAlgorithm algorithm = new GeneticAlgorithm(-1, benefitPolicy, cuboidStats);

        List<Long> recommendList = algorithm.recommend(10);
        System.out.println("recommendList by BPUSCalculator: " + recommendList);
        System.out.println("Cost evaluated for each query: " + getQueryCostRatio(cuboidStats, recommendList));
    }

    @Test
    public void testPBPUSCalculator() {
        BenefitPolicy benefitPolicy = new PBPUSCalculator(cuboidStats);
        GeneticAlgorithm algorithm = new GeneticAlgorithm(-1, benefitPolicy, cuboidStats);

        List<Long> recommendList = algorithm.recommend(10);
        System.out.println("recommendList by PBPUSCalculator:" + recommendList);
        System.out.println("Cost evaluated for each query: " + getQueryCostRatio(cuboidStats, recommendList));
    }

    @Test
    public void testSPBPUSCalculator() {
        BenefitPolicy benefitPolicy = new SPBPUSCalculator(cuboidStats);
        GeneticAlgorithm algorithm = new GeneticAlgorithm(-1, benefitPolicy, cuboidStats);

        List<Long> recommendList = algorithm.recommend(10);
        System.out.println("recommendList by SPBPUSCalculator:" + recommendList);
        System.out.println("Cost evaluated for each query: " + getQueryCostRatio(cuboidStats, recommendList));
    }
}
