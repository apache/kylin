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

package org.apache.kylin.metadata.cube.planner.algorithm;

import java.math.BigInteger;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

public class PBPUSCalculator extends BPUSCalculator {

    public PBPUSCalculator(final LayoutStats layoutStats) {
        super(layoutStats);
    }

    protected PBPUSCalculator(LayoutStats layoutStats, ImmutableMap<BigInteger, Long> initLayoutAggCostMap) {
        super(layoutStats, initLayoutAggCostMap);
    }

    @Override
    protected double getCostSaving(BigInteger descendant, BigInteger layout) {
        return getLayoutHitProb(descendant) * super.getCostSaving(descendant, layout);
    }

    protected double getLayoutHitProb(BigInteger layout) {
        return layoutStats.getLayoutHitProb(layout);
    }

    public double getMinBenefitRatio() {
        int layoutDomainSize = layoutStats.getAllLayoutsForSelection().size();
        Preconditions.checkArgument(layoutDomainSize > 0, "The set of layouts for selection is empty!!!");
        return super.getMinBenefitRatio() / layoutDomainSize;
    }

    @Override
    public BenefitPolicy getInstance() {
        return new PBPUSCalculator(this.layoutStats, this.initLayoutAggCostMap);
    }
}
