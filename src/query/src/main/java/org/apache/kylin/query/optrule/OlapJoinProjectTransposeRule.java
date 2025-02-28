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

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.rules.JoinProjectTransposeRule;
import org.apache.kylin.query.relnode.OlapJoinRel;
import org.apache.kylin.query.relnode.OlapNonEquiJoinRel;
import org.apache.kylin.query.relnode.OlapProjectRel;

public class OlapJoinProjectTransposeRule extends RelOptRule {

    private OlapJoinProjectTransposeRule(RelOptRuleOperand operand) {
        super(operand);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        throw new UnsupportedOperationException();
    }

    public static final JoinProjectTransposeRule BOTH_PROJECT = new JoinProjectTransposeRule(
            operand(OlapJoinRel.class, operand(OlapProjectRel.class, any()), operand(OlapProjectRel.class, any())),
            "JoinProjectTransposeRule(Project-Project)");

    public static final JoinProjectTransposeRule LEFT_PROJECT = new JoinProjectTransposeRule(
            operand(OlapJoinRel.class, some(operand(OlapProjectRel.class, any()))),
            "JoinProjectTransposeRule(Project-Other)");

    public static final JoinProjectTransposeRule RIGHT_PROJECT = new JoinProjectTransposeRule(
            operand(OlapJoinRel.class, operand(RelNode.class, any()), operand(OlapProjectRel.class, any())),
            "JoinProjectTransposeRule(Other-Project)");

    public static final JoinProjectTransposeRule LEFT_PROJECT_INCLUDE_OUTER = new JoinProjectTransposeRule(
            operand(OlapJoinRel.class, some(operand(OlapProjectRel.class, any()))),
            "Join(IncludingOuter)ProjectTransposeRule(Project-Other)", true, RelFactories.LOGICAL_BUILDER);

    public static final JoinProjectTransposeRule RIGHT_PROJECT_INCLUDE_OUTER = new JoinProjectTransposeRule(
            operand(OlapJoinRel.class, operand(RelNode.class, any()), operand(OlapProjectRel.class, any())),
            "Join(IncludingOuter)ProjectTransposeRule(Other-Project)", true, RelFactories.LOGICAL_BUILDER);

    public static final JoinProjectTransposeRule NON_EQUI_LEFT_PROJECT_INCLUDE_OUTER = new JoinProjectTransposeRule(
            operand(OlapNonEquiJoinRel.class, some(operand(OlapProjectRel.class, any()))),
            "Join(IncludingOuter)ProjectTransposeRule(Project-Other)", true, RelFactories.LOGICAL_BUILDER);

    public static final JoinProjectTransposeRule NON_EQUI_RIGHT_PROJECT_INCLUDE_OUTER = new JoinProjectTransposeRule(
            operand(OlapNonEquiJoinRel.class, operand(RelNode.class, any()), operand(OlapProjectRel.class, any())),
            "Join(IncludingOuter)ProjectTransposeRule(Other-Project)", true, RelFactories.LOGICAL_BUILDER);
}
