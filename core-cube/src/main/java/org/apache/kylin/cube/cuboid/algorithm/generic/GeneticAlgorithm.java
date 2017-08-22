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
import java.util.Random;

import org.apache.kylin.cube.cuboid.algorithm.AbstractRecommendAlgorithm;
import org.apache.kylin.cube.cuboid.algorithm.BenefitPolicy;
import org.apache.kylin.cube.cuboid.algorithm.generic.lib.BitsMutation;
import org.apache.kylin.cube.cuboid.algorithm.generic.lib.Chromosome;
import org.apache.kylin.cube.cuboid.algorithm.generic.lib.ChromosomePair;
import org.apache.kylin.cube.cuboid.algorithm.generic.lib.CrossoverPolicy;
import org.apache.kylin.cube.cuboid.algorithm.generic.lib.ElitisticListPopulation;
import org.apache.kylin.cube.cuboid.algorithm.generic.lib.FixedGenerationCount;
import org.apache.kylin.cube.cuboid.algorithm.generic.lib.MutationPolicy;
import org.apache.kylin.cube.cuboid.algorithm.generic.lib.OnePointCrossover;
import org.apache.kylin.cube.cuboid.algorithm.generic.lib.Population;
import org.apache.kylin.cube.cuboid.algorithm.generic.lib.SelectionPolicy;
import org.apache.kylin.cube.cuboid.algorithm.generic.lib.StoppingCondition;
import org.apache.kylin.cube.cuboid.algorithm.CuboidStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

/**
 * Implementation of a genetic algorithm to recommend a list of cuboids. All factors that govern the processing
 * of the algorithm can be configured.
 *
 */
public class GeneticAlgorithm extends AbstractRecommendAlgorithm {

    private static final Logger logger = LoggerFactory.getLogger(GeneticAlgorithm.class);
    public static ThreadLocal<Random> RANDGEN = new ThreadLocal<Random>() {
        @Override
        protected Random initialValue() {
            return new Random(System.currentTimeMillis() * Thread.currentThread().getId());
        }
    };
    /** the rate of crossover for the algorithm. */
    private final double crossoverRate = 0.9;
    /** the rate of mutation for the algorithm. */
    private final double mutationRate = 0.001;
    /** the init population size. */
    private final int populationSize = 500;
    /** the max population size. */
    private final int maxPopulationSize = 510;
    private CrossoverPolicy crossoverPolicy;
    private MutationPolicy mutationPolicy;
    private SelectionPolicy selectionPolicy;
    private CuboidEncoder cuboidEncoder;
    /** the number of generations evolved to reach {@link StoppingCondition} in the last run. */
    private int generationsEvolved = 0;

    public GeneticAlgorithm(final long timeout, BenefitPolicy benefitPolicy, CuboidStats cuboidStats) {
        super(timeout, benefitPolicy, cuboidStats);
        this.crossoverPolicy = new OnePointCrossover();
        this.mutationPolicy = new BitsMutation();
        this.selectionPolicy = new RouletteWheelSelection();

        this.cuboidEncoder = new CuboidEncoder(getCuboidStats());
    }

    @Override
    public List<Long> recommend(double expansionRate) {
        double spaceLimit = getCuboidStats().getBaseCuboidSize() * expansionRate;
        return start(spaceLimit);
    }

    @Override
    public List<Long> start(double maxSpaceLimit) {
        logger.info("Genetic Algorithm started.");

        getBenefitPolicy().initBeforeStart();

        //Initial mandatory cuboids
        double remainingSpace = maxSpaceLimit;
        for (Long mandatoryOne : getCuboidStats().getAllCuboidsForMandatory()) {
            if (getCuboidStats().getCuboidSize(mandatoryOne) != null) {
                remainingSpace -= getCuboidStats().getCuboidSize(mandatoryOne);
            }
        }

        //Generate a population randomly
        Population initial = initRandomPopulation(remainingSpace);

        //Set stopping condition
        List<StoppingCondition> conditions = Lists.newArrayList();
        conditions.add(new FixedGenerationCount(550));
        CombinedStoppingCondition stopCondition = new CombinedStoppingCondition(conditions);

        //Start the evolution
        Population current = evolve(initial, stopCondition);
        BitsChromosome chromosome = (BitsChromosome) current.getFittestChromosome();
        logger.info("Genetic Algorithm finished.");
        List<Long> finalList = Lists.newArrayList();
        finalList.addAll(getCuboidStats().getAllCuboidsForMandatory());
        finalList.addAll(cuboidEncoder.toCuboidList(chromosome.getKey()));

        double totalSpace = 0;
        if (logger.isInfoEnabled()) {
            for (Long cuboid : finalList) {
                Double unitSpace = getCuboidStats().getCuboidSize(cuboid);
                if (unitSpace != null) {
                    logger.info(String.format("cuboidId %d and Space: %f", cuboid, unitSpace));
                    totalSpace += unitSpace;
                } else {
                    logger.info(String.format("mandatory cuboidId %d", cuboid));
                }
            }
            logger.info("Total Space:" + totalSpace);
            logger.info("Space Expansion Rate:" + totalSpace / getCuboidStats().getBaseCuboidSize());
        }
        return finalList;
    }

    protected Population initRandomPopulation(double maxSpaceLimit) {
        List<Chromosome> chromosomeList = Lists.newArrayListWithCapacity(populationSize);
        List<Long> cuboidsForSelection = Lists.newArrayList(getCuboidStats().getAllCuboidsForSelection());
        int selectionSize = cuboidsForSelection.size();

        while (chromosomeList.size() < populationSize) {
            BitSet bitSetForSelection = new BitSet(selectionSize);

            //Initialize selection genes
            double totalSpace = 0;
            while (totalSpace < maxSpaceLimit) {
                int j = RANDGEN.get().nextInt(selectionSize);
                if (!bitSetForSelection.get(j)) {
                    totalSpace += getCuboidStats().getCuboidSize(cuboidsForSelection.get(j));
                    bitSetForSelection.set(j);
                }
            }

            Chromosome chromosome = new BitsChromosome(bitSetForSelection, getBenefitPolicy(), getCuboidStats(),
                    maxSpaceLimit);
            chromosomeList.add(chromosome);
        }
        return new ElitisticListPopulation(chromosomeList, maxPopulationSize, 0.8);
    }

    /**
     * Evolve the given population. Evolution stops when the stopping condition
     * is satisfied. Updates the {@link #getGenerationsEvolved() generationsEvolved}
     * property with the number of generations evolved before the StoppingCondition
     * is satisfied.
     *
     * @param initial the initial, seed population.
     * @param condition the stopping condition used to stop evolution.
     * @return the population that satisfies the stopping condition.
     */
    protected Population evolve(final Population initial, final StoppingCondition condition) {
        Population current = initial;
        generationsEvolved = 0;
        while (!condition.isSatisfied(current) && (!shouldCancel())) {
            current = nextGeneration(current);
            generationsEvolved++;
            logger.info("Generations evolved count:" + generationsEvolved);
        }
        return current;
    }

    /**
     * Evolve the given population into the next generation.
     * <p>
     * <ol>
     *  <li>Get nextGeneration population to fill from <code>current</code>
     *      generation, using its nextGeneration method</li>
     *  <li>Loop until new generation is filled:</li>
     *  <ul><li>Apply configured SelectionPolicy to select a pair of parents
     *          from <code>current</code></li>
     *      <li>With probability = {@link #getCrossoverRate()}, apply
     *          configured {@link CrossoverPolicy} to parents</li>
     *      <li>With probability = {@link #getMutationRate()}, apply
     *          configured {@link MutationPolicy} to each of the offspring</li>
     *      <li>Add offspring individually to nextGeneration,
     *          space permitting</li>
     *  </ul>
     *  <li>Return nextGeneration</li>
     * </ol>
     *
     * @param current the current population.
     * @return the population for the next generation.
     */
    protected Population nextGeneration(final Population current) {
        Population nextGeneration = current.nextGeneration();

        while (nextGeneration.getPopulationSize() < nextGeneration.getPopulationLimit()) {
            // select parent chromosomes
            ChromosomePair pair = getSelectionPolicy().select(current);

            // crossover?
            if (RANDGEN.get().nextDouble() < getCrossoverRate()) {
                // apply crossover policy to create two offspring
                pair = getCrossoverPolicy().crossover(pair.getFirst(), pair.getSecond());
            }

            // mutation?
            if (RANDGEN.get().nextDouble() < getMutationRate()) {
                // apply mutation policy to the chromosomes
                pair = new ChromosomePair(getMutationPolicy().mutate(pair.getFirst()),
                        getMutationPolicy().mutate(pair.getSecond()));
            }

            // add the first chromosome to the population
            nextGeneration.addChromosome(pair.getFirst());
            // is there still a place for the second chromosome?
            if (nextGeneration.getPopulationSize() < nextGeneration.getPopulationLimit()) {
                // add the second chromosome to the population
                nextGeneration.addChromosome(pair.getSecond());
            }
        }
        return nextGeneration;
    }

    /**
     * Returns the crossover policy.
     * @return crossover policy
     */
    public CrossoverPolicy getCrossoverPolicy() {
        return crossoverPolicy;
    }

    /**
     * Returns the crossover rate.
     * @return crossover rate
     */
    public double getCrossoverRate() {
        return crossoverRate;
    }

    /**
     * Returns the mutation policy.
     * @return mutation policy
     */
    public MutationPolicy getMutationPolicy() {
        return mutationPolicy;
    }

    /**
     * Returns the mutation rate.
     * @return mutation rate
     */
    public double getMutationRate() {
        return mutationRate;
    }

    /**
     * Returns the selection policy.
     * @return selection policy
     */
    public SelectionPolicy getSelectionPolicy() {
        return selectionPolicy;
    }

    /**
     * Returns the number of generations evolved to reach {@link StoppingCondition} in the last run.
     *
     * @return number of generations evolved
     */
    public int getGenerationsEvolved() {
        return generationsEvolved;
    }

}
