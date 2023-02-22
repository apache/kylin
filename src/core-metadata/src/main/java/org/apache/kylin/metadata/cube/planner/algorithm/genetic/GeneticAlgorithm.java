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

import java.math.BigInteger;
import java.util.BitSet;
import java.util.List;
import java.util.Locale;

import org.apache.commons.math3.genetics.Chromosome;
import org.apache.commons.math3.genetics.ElitisticListPopulation;
import org.apache.commons.math3.genetics.FixedGenerationCount;
import org.apache.commons.math3.genetics.Population;
import org.apache.commons.math3.genetics.StoppingCondition;
import org.apache.kylin.metadata.cube.planner.algorithm.AbstractRecommendAlgorithm;
import org.apache.kylin.metadata.cube.planner.algorithm.BenefitPolicy;
import org.apache.kylin.metadata.cube.planner.algorithm.LayoutStats;

import com.google.common.collect.Lists;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class GeneticAlgorithm extends AbstractRecommendAlgorithm {

    private final org.apache.commons.math3.genetics.GeneticAlgorithm geneticAlgorithm;

    /**
     * the rate of crossover for the algorithm.
     */
    private final double crossoverRate = 0.9;
    /**
     * the rate of mutation for the algorithm.
     */
    private final double mutationRate = 0.001;
    /**
     * the init population size.
     */
    private final int populationSize = 500;
    /**
     * the max population size.
     */
    private final int maxPopulationSize = 510;

    public GeneticAlgorithm(final long timeout, BenefitPolicy benefitPolicy, LayoutStats layoutStats) {
        super(timeout, benefitPolicy, layoutStats);
        this.geneticAlgorithm = new org.apache.commons.math3.genetics.GeneticAlgorithm(new BitsOnePointCrossover(),
                crossoverRate, new BitsMutation(), mutationRate, new RouletteWheelSelection());
    }

    @Override
    public List<BigInteger> start(double maxSpaceLimit) {
        log.debug("Genetic Algorithm started.");

        //Initial mandatory layouts
        double remainingSpace = maxSpaceLimit;
        for (BigInteger mandatoryOne : layoutStats.getAllLayoutsForMandatory()) {
            if (layoutStats.getLayoutSize(mandatoryOne) != null) {
                remainingSpace -= layoutStats.getLayoutSize(mandatoryOne);
            }
        }

        BitsChromosomeHelper helper = new BitsChromosomeHelper(remainingSpace, layoutStats);

        //Generate a population randomly
        Population initial = initRandomPopulation(helper);

        //Set stopping condition
        List<StoppingCondition> conditions = Lists.newArrayList();
        conditions.add(new FixedGenerationCount(550));
        CombinedStoppingCondition stopCondition = new CombinedStoppingCondition(conditions);

        //Start the evolution
        Population current = geneticAlgorithm.evolve(initial, stopCondition);
        BitsChromosome chromosome = (BitsChromosome) current.getFittestChromosome();
        log.debug("Genetic Algorithm finished.");
        List<BigInteger> finalList = Lists.newArrayList();
        finalList.addAll(helper.getMandatoryLayouts());
        finalList.addAll(chromosome.getLayouts());

        double totalSpace = 0;
        if (log.isTraceEnabled()) {
            for (BigInteger layout : finalList) {
                Double unitSpace = layoutStats.getLayoutSize(layout);
                if (unitSpace != null) {
                    log.trace(String.format(Locale.ROOT, "layoutId %d and Space: %f", layout, unitSpace));
                    totalSpace += unitSpace;
                } else {
                    log.trace(String.format(Locale.ROOT, "mandatory layoutId %d", layout));
                }
            }
            log.trace("Total Space:" + totalSpace);
            log.trace("Space Expansion Rate:" + totalSpace / layoutStats.getBaseLayoutSize());
        }
        return finalList;
    }

    protected Population initRandomPopulation(BitsChromosomeHelper helper) {
        List<Chromosome> chromosomeList = Lists.newArrayListWithCapacity(populationSize);

        while (chromosomeList.size() < populationSize) {
            BitSet bitSetForSelection = new BitSet(helper.getLength());

            //Initialize selection genes
            double totalSpace = 0;
            while (totalSpace < helper.spaceLimit) {
                int j = org.apache.commons.math3.genetics.GeneticAlgorithm.getRandomGenerator()
                        .nextInt(helper.getLength());
                if (!bitSetForSelection.get(j)) {
                    totalSpace += helper.getLayoutSizeByBitIndex(j);
                    bitSetForSelection.set(j);
                }
            }

            Chromosome chromosome = new BitsChromosome(bitSetForSelection, benefitPolicy.getInstance(), helper);
            chromosomeList.add(chromosome);
        }
        return new ElitisticListPopulation(chromosomeList, maxPopulationSize, 0.8);
    }
}
