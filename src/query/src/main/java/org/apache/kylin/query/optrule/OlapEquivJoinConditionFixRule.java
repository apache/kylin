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

import java.util.List;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.kylin.query.relnode.OlapJoinRel;
import org.apache.kylin.query.util.RexUtils;

/**
 * correct the join conditions of equal join
 * 1. Remove casts. This rule will search the join conditions
 * and replace condition like cast(col1 as ...) = col2 with col1 = col2
 */
public class OlapEquivJoinConditionFixRule extends RelOptRule {

    public static final OlapEquivJoinConditionFixRule INSTANCE = new OlapEquivJoinConditionFixRule();

    private OlapEquivJoinConditionFixRule() {
        super(operand(OlapJoinRel.class, any()), RelFactories.LOGICAL_BUILDER, "OlapEquivJoinConditionFixRule:join");
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        OlapJoinRel join = call.rel(0);
        List<RexNode> conditions = RelOptUtil.conjunctions(join.getCondition());
        if (conditions.isEmpty()) {
            return;
        }

        boolean conditionModified = false;
        for (int i = 0; i < conditions.size(); i++) {
            RexNode stripped = RexUtils.stripOffCastInColumnEqualPredicate(conditions.get(i));
            if (stripped != conditions.get(i)) {
                conditionModified = true;
                conditions.set(i, stripped);
            }
        }

        if (conditionModified) {
            final RexBuilder rexBuilder = join.getCluster().getRexBuilder();
            RexNode composed = RexUtil.composeConjunction(rexBuilder, conditions, false);
            call.transformTo(join.copy(join.getTraitSet(), composed, join.getLeft(), join.getRight(),
                    join.getJoinType(), join.isSemiJoinDone()));
        }
    }
}
