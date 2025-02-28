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

package org.apache.kylin.query.optrule;

import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.rules.ReduceExpressionsRule;

/**
 * Override {@link ReduceExpressionsRule} so that it can reduce expressions of OlapRel
 */
public class OlapReduceExpressionRule {
    private OlapReduceExpressionRule() {
    }

    public static final ReduceExpressionsRule FILTER_INSTANCE = new ReduceExpressionsRule.FilterReduceExpressionsRule(
            Filter.class, true, RelFactories.LOGICAL_BUILDER);

    public static final ReduceExpressionsRule PROJECT_INSTANCE = new ReduceExpressionsRule.ProjectReduceExpressionsRule(
            Project.class, true, RelFactories.LOGICAL_BUILDER);

    public static final ReduceExpressionsRule JOIN_INSTANCE = new ReduceExpressionsRule.JoinReduceExpressionsRule(
            Join.class, true, RelFactories.LOGICAL_BUILDER);

    public static final ReduceExpressionsRule CALC_INSTANCE = new ReduceExpressionsRule.CalcReduceExpressionsRule(
            Calc.class, true, RelFactories.LOGICAL_BUILDER);

}
