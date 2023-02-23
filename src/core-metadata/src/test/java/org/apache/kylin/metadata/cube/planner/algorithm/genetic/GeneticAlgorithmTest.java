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
package org.apache.kylin.metadata.cube.planner.algorithm.genetic;

import static org.junit.Assert.assertEquals;

import java.math.BigInteger;
import java.util.BitSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.math3.genetics.Chromosome;
import org.apache.kylin.metadata.cube.planner.algorithm.AlgorithmTestBase;
import org.apache.kylin.metadata.cube.planner.algorithm.BPUSCalculator;
import org.apache.kylin.metadata.cube.planner.algorithm.BenefitPolicy;
import org.apache.kylin.metadata.cube.planner.algorithm.PBPUSCalculator;
import org.apache.kylin.metadata.cube.planner.algorithm.SPBPUSCalculator;
import org.junit.Test;

import com.google.common.collect.Sets;

public class GeneticAlgorithmTest extends AlgorithmTestBase {

    @Test
    public void testChromosomeIsSame() {
        BenefitPolicy benefitPolicy = new BPUSCalculator(layoutStats);

        double maxSpaceLimit = layoutStats.getBaseLayoutSize() * 10;
        BitsChromosomeHelper helper = new BitsChromosomeHelper(maxSpaceLimit, layoutStats);

        double maxSpaceLimit1 = layoutStats.getBaseLayoutSize() * 12;
        BitsChromosomeHelper helper1 = new BitsChromosomeHelper(maxSpaceLimit1, layoutStats);

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
        BenefitPolicy benefitPolicy = new BPUSCalculator(layoutStats);
        GeneticAlgorithm algorithm = new GeneticAlgorithm(-1, benefitPolicy, layoutStats);

        List<BigInteger> recommendList = algorithm.recommend(10);
        System.out.println("recommendList by BPUSCalculator: " + recommendList);
        System.out.println("Cost evaluated for each query: " + getQueryCostRatio(layoutStats, recommendList));
    }

    @Test
    public void testPBPUSCalculator() {
        BenefitPolicy benefitPolicy = new PBPUSCalculator(layoutStats);
        GeneticAlgorithm algorithm = new GeneticAlgorithm(-1, benefitPolicy, layoutStats);

        List<BigInteger> recommendList = algorithm.recommend(10);
        System.out.println("recommendList by PBPUSCalculator:" + recommendList);
        System.out.println("Cost evaluated for each query: " + getQueryCostRatio(layoutStats, recommendList));
    }

    @Test
    public void testSPBPUSCalculator() {
        BenefitPolicy benefitPolicy = new SPBPUSCalculator(layoutStats);
        GeneticAlgorithm algorithm = new GeneticAlgorithm(-1, benefitPolicy, layoutStats);

        List<BigInteger> recommendList = algorithm.recommend(10);
        System.out.println("recommendList by SPBPUSCalculator:" + recommendList);
        System.out.println("Cost evaluated for each query: " + getQueryCostRatio(layoutStats, recommendList));
    }
}
