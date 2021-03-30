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

package org.apache.kylin.cube.cuboid.algorithm.generic;

import java.util.BitSet;
import java.util.List;
import java.util.Locale;

import org.apache.commons.math3.genetics.Chromosome;
import org.apache.commons.math3.genetics.ElitisticListPopulation;
import org.apache.commons.math3.genetics.FixedGenerationCount;
import org.apache.commons.math3.genetics.Population;
import org.apache.commons.math3.genetics.StoppingCondition;
import org.apache.kylin.cube.cuboid.algorithm.AbstractRecommendAlgorithm;
import org.apache.kylin.cube.cuboid.algorithm.BenefitPolicy;
import org.apache.kylin.cube.cuboid.algorithm.CuboidStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kylin.shaded.com.google.common.collect.Lists;

/**
 * Implementation of a genetic algorithm to recommend a list of cuboids.
 * All factors that govern the processing of the algorithm can be configured.
 */
public class GeneticAlgorithm extends AbstractRecommendAlgorithm {

    private static final Logger logger = LoggerFactory.getLogger(GeneticAlgorithm.class);

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

    public GeneticAlgorithm(final long timeout, BenefitPolicy benefitPolicy, CuboidStats cuboidStats) {
        super(timeout, benefitPolicy, cuboidStats);
        this.geneticAlgorithm = new org.apache.commons.math3.genetics.GeneticAlgorithm(new BitsOnePointCrossover(),
                crossoverRate, new BitsMutation(), mutationRate, new RouletteWheelSelection());
    }

    @Override
    public List<Long> start(double maxSpaceLimit) {
        logger.debug("Genetic Algorithm started.");

        //Initial mandatory cuboids
        double remainingSpace = maxSpaceLimit;
        for (Long mandatoryOne : cuboidStats.getAllCuboidsForMandatory()) {
            if (cuboidStats.getCuboidSize(mandatoryOne) != null) {
                remainingSpace -= cuboidStats.getCuboidSize(mandatoryOne);
            }
        }

        BitsChromosomeHelper helper = new BitsChromosomeHelper(remainingSpace, cuboidStats);

        //Generate a population randomly
        Population initial = initRandomPopulation(helper);

        //Set stopping condition
        List<StoppingCondition> conditions = Lists.newArrayList();
        conditions.add(new FixedGenerationCount(550));
        CombinedStoppingCondition stopCondition = new CombinedStoppingCondition(conditions);

        //Start the evolution
        Population current = geneticAlgorithm.evolve(initial, stopCondition);
        BitsChromosome chromosome = (BitsChromosome) current.getFittestChromosome();
        logger.debug("Genetic Algorithm finished.");
        List<Long> finalList = Lists.newArrayList();
        finalList.addAll(helper.getMandatoryCuboids());
        finalList.addAll(chromosome.getCuboids());

        double totalSpace = 0;
        if (logger.isTraceEnabled()) {
            for (Long cuboid : finalList) {
                Double unitSpace = cuboidStats.getCuboidSize(cuboid);
                if (unitSpace != null) {
                    logger.trace(String.format(Locale.ROOT, "cuboidId %d and Space: %f", cuboid, unitSpace));
                    totalSpace += unitSpace;
                } else {
                    logger.trace(String.format(Locale.ROOT, "mandatory cuboidId %d", cuboid));
                }
            }
            logger.trace("Total Space:" + totalSpace);
            logger.trace("Space Expansion Rate:" + totalSpace / cuboidStats.getBaseCuboidSize());
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
                    totalSpace += helper.getCuboidSizeByBitIndex(j);
                    bitSetForSelection.set(j);
                }
            }

            Chromosome chromosome = new BitsChromosome(bitSetForSelection, benefitPolicy.getInstance(), helper);
            chromosomeList.add(chromosome);
        }
        return new ElitisticListPopulation(chromosomeList, maxPopulationSize, 0.8);
    }
}
