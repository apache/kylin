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

package org.apache.kylin.cube.cuboid.algorithm.generic.lib;

import java.util.BitSet;

import org.apache.kylin.cube.cuboid.algorithm.generic.BitsChromosome;
import org.apache.kylin.cube.cuboid.algorithm.generic.GeneticAlgorithm;

/**
 * One point crossover policy. A random crossover point is selected and the
 * first part from each parent is copied to the corresponding child, and the
 * second parts are copied crosswise.
 *
 * Example:
 * <pre>
 * -C- denotes a crossover point
 *                   -C-                                 -C-
 * p1 = (1 0 1 0 0 1  | 0 1 1)    X    p2 = (0 1 1 0 1 0  | 1 1 1)
 *      \------------/ \-----/              \------------/ \-----/
 *            ||         (*)                       ||        (**)
 *            VV         (**)                      VV        (*)
 *      /------------\ /-----\              /------------\ /-----\
 * c1 = (1 0 1 0 0 1  | 1 1 1)    X    c2 = (0 1 1 0 1 0  | 0 1 1)
 * </pre>
 *
 * This policy works only on {@link BitsChromosome}, and therefore it
 * is parameterized by T. Moreover, the chromosomes must have same lengths.
 *
 *
 */
public class OnePointCrossover implements CrossoverPolicy {

    /**
     * Performs one point crossover. A random crossover point is selected and the
     * first part from each parent is copied to the corresponding child, and the
     * second parts are copied crosswise.
     *
     * Example:
     * <pre>
     * -C- denotes a crossover point
     *                   -C-                                 -C-
     * p1 = (1 0 1 0 0 1  | 0 1 1)    X    p2 = (0 1 1 0 1 0  | 1 1 1)
     *      \------------/ \-----/              \------------/ \-----/
     *            ||         (*)                       ||        (**)
     *            VV         (**)                      VV        (*)
     *      /------------\ /-----\              /------------\ /-----\
     * c1 = (1 0 1 0 0 1  | 1 1 1)    X    c2 = (0 1 1 0 1 0  | 0 1 1)
     * </pre>
     *
     * @param first first parent (p1)
     * @param second second parent (p2)
     * @return pair of two children (c1,c2)
     * @throws IllegalArgumentException if one of the chromosomes is
     *   not an instance of {@link BitsChromosome}
     * @throws ChromosomeMismatchException if the length of the two chromosomes is different
     */
    @SuppressWarnings("unchecked") // OK because of instanceof checks
    public ChromosomePair crossover(final Chromosome first, final Chromosome second)
            throws IllegalArgumentException, ChromosomeMismatchException {

        if (!(first instanceof BitsChromosome && second instanceof BitsChromosome)) {
            throw new IllegalArgumentException("Chromosome first " + first.getClass() + " and second "
                    + second.getClass() + " must be of type BitsChromosome.");
        }
        return crossover((BitsChromosome) first, (BitsChromosome) second);
    }

    /**
     * Helper for {@link #crossover(Chromosome, Chromosome)}. Performs the actual crossover.
     *
     * @param first the first chromosome.
     * @param second the second chromosome.
     * @return the pair of new chromosomes that resulted from the crossover.
     * @throws ChromosomeMismatchException if the length of the two chromosomes is different
     */
    private ChromosomePair crossover(final BitsChromosome first, final BitsChromosome second)
            throws ChromosomeMismatchException {
        final int length = first.getLength();
        if (length != second.getLength()) {
            throw new ChromosomeMismatchException("BitsChromosome length mismatch.", second.getLength(), length);
        }

        final BitSet parent1Key = first.getKey();
        final BitSet parent2Key = second.getKey();

        final BitSet child1Key = new BitSet(length);
        final BitSet child2Key = new BitSet(length);

        // select a crossover point at random (0 and length makes no sense)
        final int crossoverIndex = 1 + (GeneticAlgorithm.RANDGEN.get().nextInt(length - 2));

        BitSet a = (BitSet) parent1Key.clone();
        a.clear(crossoverIndex, length);
        BitSet b = (BitSet) parent2Key.clone();
        b.clear(0, crossoverIndex);

        BitSet c = (BitSet) parent1Key.clone();
        c.clear(crossoverIndex, length);
        BitSet d = (BitSet) parent2Key.clone();
        d.clear(0, crossoverIndex);

        child1Key.or(a);
        child1Key.or(d);

        child2Key.or(c);
        child2Key.or(b);
        return new ChromosomePair(first.newBitsChromosome(child1Key), second.newBitsChromosome(child2Key));
    }
}
