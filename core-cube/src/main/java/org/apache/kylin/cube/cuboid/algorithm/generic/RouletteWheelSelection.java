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

import org.apache.kylin.shaded.com.google.common.collect.Lists;
import org.apache.commons.math3.genetics.Chromosome;
import org.apache.commons.math3.genetics.ChromosomePair;
import org.apache.commons.math3.genetics.GeneticAlgorithm;
import org.apache.commons.math3.genetics.ListPopulation;
import org.apache.commons.math3.genetics.Population;
import org.apache.commons.math3.genetics.SelectionPolicy;

import java.util.List;

public class RouletteWheelSelection implements SelectionPolicy {

    @Override
    public ChromosomePair select(Population population) throws IllegalArgumentException {
        // create a copy of the chromosome list
        List<Chromosome> chromosomes = Lists.newArrayList(((ListPopulation) population).getChromosomes());

        double maxFitness = 0;
        double totalFitness = 0;
        for (Chromosome o : chromosomes) {
            double fitness = o.getFitness();
            totalFitness += fitness;
            if (fitness > maxFitness) {
                maxFitness = fitness;
            }
        }
        return new ChromosomePair(rouletteWheel(chromosomes, totalFitness), rouletteWheel(chromosomes, totalFitness));
    }

    private Chromosome rouletteWheel(final List<Chromosome> chromosomes, final double totalFitness) {
        double rnd = (GeneticAlgorithm.getRandomGenerator().nextDouble() * totalFitness);
        double runningScore = 0;
        for (Chromosome o : chromosomes) {
            if (rnd >= runningScore && rnd <= runningScore + o.getFitness()) {
                return o;
            }
            runningScore += o.getFitness();
        }
        return null;
    }
}
