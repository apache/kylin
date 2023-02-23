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
import java.util.Set;

public interface BenefitPolicy {

    /**
     * @return a cloned instance with initial status
     */
    BenefitPolicy getInstance();

    /**
     * @param selected should not be changed
     *                 Will not change the inner instance status
     */
    LayoutBenefitModel.BenefitModel calculateBenefit(BigInteger layout, Set<BigInteger> selected);

    /**
     * @param layoutsToAdd should not be changed
     * @param selected     should not be changed
     *                     Will not change the inner instance status
     */
    LayoutBenefitModel.BenefitModel calculateBenefitTotal(Set<BigInteger> layoutsToAdd, Set<BigInteger> selected);

    boolean ifEfficient(LayoutBenefitModel best);

    /**
     * @param selected should not be changed
     * Will update the inner instance status
     */
    void propagateAggregationCost(BigInteger layout, Set<BigInteger> selected);
}
