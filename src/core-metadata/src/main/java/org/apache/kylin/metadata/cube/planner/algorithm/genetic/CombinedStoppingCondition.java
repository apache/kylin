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

import java.util.List;

import org.apache.commons.math3.genetics.Population;
import org.apache.commons.math3.genetics.StoppingCondition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CombinedStoppingCondition implements StoppingCondition {
    private static final Logger logger = LoggerFactory.getLogger(GeneticAlgorithm.class);

    private final List<StoppingCondition> conditions;

    public CombinedStoppingCondition(List<StoppingCondition> conditions) {
        this.conditions = conditions;
    }

    @Override
    public boolean isSatisfied(Population population) {
        for (StoppingCondition condition : conditions) {
            if (condition.isSatisfied(population)) {
                logger.info("Stopping condition {} is satisfied", condition);
                return true;
            }
        }
        return false;
    }
}
