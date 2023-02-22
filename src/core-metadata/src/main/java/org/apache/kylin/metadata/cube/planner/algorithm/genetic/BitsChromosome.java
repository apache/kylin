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
import java.util.Objects;

import org.apache.commons.math3.genetics.Chromosome;
import org.apache.kylin.metadata.cube.planner.algorithm.BenefitPolicy;
import org.apache.kylin.metadata.cube.planner.algorithm.LayoutBenefitModel;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

public class BitsChromosome extends Chromosome {

    /**
     * Global unmodified
     */
    private final BitsChromosomeHelper helper;

    /**
     * BitSet representing the chromosome
     */
    private final BitSet representation;
    private final ImmutableSet<BigInteger> layouts;

    private final BenefitPolicy benefitPolicy;

    private final double spaceCost;

    public BitsChromosome(final BitSet representation, final BenefitPolicy benefitPolicy, BitsChromosomeHelper helper) {
        this.helper = helper;

        this.representation = representation;
        this.layouts = ImmutableSet.copyOf(helper.toLayoutList(representation));

        this.benefitPolicy = benefitPolicy;

        this.spaceCost = helper.getLayoutSize(Sets.newHashSet(layouts));
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

    public ImmutableSet<BigInteger> getLayouts() {
        return layouts;
    }

    @Override
    public synchronized double fitness() {
        LayoutBenefitModel.BenefitModel benefitModel = benefitPolicy.calculateBenefitTotal(layouts,
                helper.getMandatoryLayouts());
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

        if (!Objects.equals(helper, that.helper))
            return false;
        return Objects.equals(representation, that.representation);

    }

    @Override
    public int hashCode() {
        int result = helper != null ? helper.hashCode() : 0;
        result = 31 * result + (representation != null ? representation.hashCode() : 0);
        return result;
    }
}
