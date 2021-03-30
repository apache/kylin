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

import org.apache.kylin.shaded.com.google.common.collect.ImmutableSet;
import org.apache.kylin.shaded.com.google.common.collect.Sets;
import org.apache.commons.math3.genetics.Chromosome;
import org.apache.kylin.cube.cuboid.algorithm.BenefitPolicy;
import org.apache.kylin.cube.cuboid.algorithm.CuboidBenefitModel;

import java.util.BitSet;

public class BitsChromosome extends Chromosome {

    /**
     * Global unmodified
     */
    private final BitsChromosomeHelper helper;

    /**
     * BitSet representing the chromosome
     */
    private final BitSet representation;
    private final ImmutableSet<Long> cuboids;

    private final BenefitPolicy benefitPolicy;

    private final double spaceCost;

    public BitsChromosome(final BitSet representation, final BenefitPolicy benefitPolicy, BitsChromosomeHelper helper) {
        this.helper = helper;

        this.representation = representation;
        this.cuboids = ImmutableSet.copyOf(helper.toCuboidList(representation));

        this.benefitPolicy = benefitPolicy;

        this.spaceCost = helper.getCuboidSize(Sets.newHashSet(cuboids));
    }

    public BitsChromosome newBitsChromosome(BitSet newRepresentation) {
        return new BitsChromosome(newRepresentation, benefitPolicy.getInstance(), helper);
    }

    public BitSet getRepresentation() {
        return representation;
    }

    /**
     * Returns the length of the chromosome.
     *
     * @return the length of the chromosome
     */
    public int getLength() {
        return helper.getLength();
    }

    public ImmutableSet<Long> getCuboids() {
        return cuboids;
    }

    @Override
    public synchronized double fitness() {
        CuboidBenefitModel.BenefitModel benefitModel = benefitPolicy.calculateBenefitTotal(cuboids,
                helper.getMandatoryCuboids());
        double totalBenefit = benefitModel.benefit;
        if (spaceCost > helper.spaceLimit) {
            totalBenefit = totalBenefit * helper.spaceLimit / spaceCost;
        }
        return totalBenefit;
    }

    @Override
    protected boolean isSame(final Chromosome another) {
        return this.equals(another);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        BitsChromosome that = (BitsChromosome) o;

        if (helper != null ? !helper.equals(that.helper) : that.helper != null)
            return false;
        return representation != null ? representation.equals(that.representation) : that.representation == null;

    }

    @Override
    public int hashCode() {
        int result = helper != null ? helper.hashCode() : 0;
        result = 31 * result + (representation != null ? representation.hashCode() : 0);
        return result;
    }
}
