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

package org.apache.kylin.metadata.realization;

import java.util.List;

import org.apache.kylin.metadata.model.FunctionDesc;

import com.google.common.collect.Lists;

public class CapabilityResult {

    /** Is capable or not */
    public boolean capable;

    /** The smaller the cost, the more capable the realization */
    public int cost;

    /**
     * Marker objects to indicate all special features
     * (dimension-as-measure, topN etc.) that have influenced the capability check.
     */
    public List<CapabilityInfluence> influences = Lists.newArrayListWithCapacity(1);

    public static interface CapabilityInfluence {
        /** Suggest a multiplier to influence query cost */
        double suggestCostMultiplier();
    }

    public static class DimensionAsMeasure implements CapabilityInfluence {

        final FunctionDesc function;

        public DimensionAsMeasure(FunctionDesc function) {
            this.function = function;
        }

        @Override
        public double suggestCostMultiplier() {
            return 1;
        }

        public FunctionDesc getMeasureFunction() {
            return function;
        }
    }
}
