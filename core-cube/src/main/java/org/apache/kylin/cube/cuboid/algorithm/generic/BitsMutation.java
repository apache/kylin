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

import org.apache.commons.math3.exception.MathIllegalArgumentException;
import org.apache.commons.math3.exception.util.DummyLocalizable;
import org.apache.commons.math3.genetics.Chromosome;
import org.apache.commons.math3.genetics.GeneticAlgorithm;
import org.apache.commons.math3.genetics.MutationPolicy;

import java.util.BitSet;

/**
 * Modified from the BinaryMutation.java in https://github.com/apache/commons-math
 * <p>
 * Mutation for {@link BitsChromosome}s. Randomly changes one gene.
 */
public class BitsMutation implements MutationPolicy {

    /**
     * Mutate the given chromosome. Randomly changes one gene.
     *
     * @param original the original chromosome.
     * @return the mutated chromosome.
     * @throws IllegalArgumentException if <code>original</code> is not an instance of {@link BitsChromosome}.
     */
    public Chromosome mutate(Chromosome original) throws IllegalArgumentException {
        if (!(original instanceof BitsChromosome)) {
            throw new MathIllegalArgumentException(new DummyLocalizable("bits mutation only works on BitsChromosome"));
        }

        BitsChromosome origChrom = (BitsChromosome) original;
        BitSet newNey = (BitSet) origChrom.getRepresentation().clone();

        // randomly select a gene
        int geneIndex = GeneticAlgorithm.getRandomGenerator().nextInt(origChrom.getLength());
        // change it
        newNey.set(geneIndex, !newNey.get(geneIndex));

        Chromosome newChrom = origChrom.newBitsChromosome(newNey);
        return newChrom;
    }
}
