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

import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.rules.AggregateReduceFunctionsRule;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.tools.RelBuilderFactory;

public class OlapAggregateReduceFunctionsRule extends AggregateReduceFunctionsRule {
    /** The singleton. */
    public static final AggregateReduceFunctionsRule INSTANCE = new OlapAggregateReduceFunctionsRule(
            operand(LogicalAggregate.class, any()), RelFactories.LOGICAL_BUILDER);

    //~ Constructors -----------------------------------------------------------

    /** Creates an AggregateReduceFunctionsRule. */
    private OlapAggregateReduceFunctionsRule(RelOptRuleOperand operand, RelBuilderFactory relBuilderFactory) {
        super(operand, relBuilderFactory);
    }

    @Override
    public boolean canReduce(AggregateCall call) {
        return SqlKind.AVG_AGG_FUNCTIONS.contains(call.getAggregation().getKind());
    }
}
