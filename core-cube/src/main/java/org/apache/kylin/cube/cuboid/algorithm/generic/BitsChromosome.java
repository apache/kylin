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
import java.util.Set;

import org.apache.kylin.cube.cuboid.algorithm.BenefitPolicy;
import org.apache.kylin.cube.cuboid.algorithm.CuboidBenefitModel;
import org.apache.kylin.cube.cuboid.algorithm.generic.lib.Chromosome;
import org.apache.kylin.cube.cuboid.algorithm.CuboidStats;

import com.google.common.collect.Sets;

public class BitsChromosome extends Chromosome {

    private BitSet key;
    private int length;
    private BenefitPolicy benefitPolicy;
    private CuboidStats cuboidStats;
    private CuboidEncoder cuboidEncoder;
    private double spaceLimit;
    private double spaceCost;
    private double fitness = -1;

    public BitsChromosome(final BitSet key, final BenefitPolicy benefitPolicy, final CuboidStats cuboidStats,
            final double spaceLimit) {
        super();
        this.key = key;
        this.length = cuboidStats.getAllCuboidsForSelection().size();
        this.benefitPolicy = benefitPolicy.getInstance();
        this.cuboidStats = cuboidStats;
        this.cuboidEncoder = new CuboidEncoder(cuboidStats);
        this.spaceLimit = spaceLimit;
        initSpaceCost();
    }

    public BitsChromosome newBitsChromosome(BitSet newkey) {
        return new BitsChromosome(newkey, this.benefitPolicy, this.cuboidStats, this.spaceLimit);
    }

    private void initSpaceCost() {
        spaceCost = 0;
        List<Long> remainingCuboids = cuboidEncoder.toCuboidList(key);
        for (Long cuboid : remainingCuboids) {
            spaceCost += cuboidStats.getCuboidSize(cuboid);
        }
    }

    public BitSet getKey() {
        return key;
    }

    public int getLength() {
        return length;
    }

    public CuboidEncoder getCuboidEncoder() {
        return cuboidEncoder;
    }

    @Override
    public double fitness() {
        if (fitness == -1) {
            fitness = calculateFitness();
        }
        return fitness;
    }

    @Override
    protected boolean isSame(final Chromosome another) {
        return this.equals(another);
    }

    private synchronized double calculateFitness() {
        List<Long> remainingCuboids = cuboidEncoder.toCuboidList(key);
        Set<Long> selectedCuboidSets = Sets.newHashSet();
        selectedCuboidSets.addAll(cuboidStats.getAllCuboidsForMandatory());

        CuboidBenefitModel.BenefitModel benefitModel = benefitPolicy.calculateBenefitTotal(remainingCuboids, selectedCuboidSets);
        double totalBenefit = benefitModel.getBenefit();
        if (spaceCost > spaceLimit) {
            totalBenefit = totalBenefit * spaceLimit / spaceCost;
        }
        return totalBenefit;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((key == null) ? 0 : key.hashCode());
        result = prime * result + length;
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        BitsChromosome other = (BitsChromosome) obj;
        if (length != other.length) {
            return false;
        }
        if (key == null) {
            if (other.key != null)
                return false;
        } else if (!key.equals(other.key))
            return false;
        return true;
    }
}
