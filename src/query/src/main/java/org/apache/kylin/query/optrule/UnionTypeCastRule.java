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

import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.SetOp;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.Pair;
import org.apache.kylin.query.relnode.OlapProjectRel;
import org.apache.kylin.query.relnode.OlapUnionRel;

/**
 * sql: select TRANS_ID from KYLIN_SALES union all select LSTG_SITE_ID from KYLIN_SALES
 *
 * EXECUTION PLAN:
 * OlapUnionRel(all=[true], ctx=[], all=[true])
 *   OlapProjectRel(TRANS_ID=[$0], ctx=[])
 *     OlapTableScan(table=[[DEFAULT, KYLIN_SALES]], ctx=[], fields=[[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13]])
 *   OlapProjectRel(LSTG_SITE_ID=[$4], ctx=[])
 *     OlapTableScan(table=[[DEFAULT, KYLIN_SALES]], ctx=[], fields=[[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13]])
 *
 * Apply this rule to convert execution plan.
 * After convert:
 *
 * OlapUnionRel(all=[true], ctx=[], all=[true])
 *   OlapProjectRel(TRANS_ID=[$0], ctx=[])
 *     OlapTableScan(table=[[DEFAULT, KYLIN_SALES]], ctx=[], fields=[[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13]])
 *   OlapProjectRel(TRANS_ID=[CAST($4):BIGINT], ctx=[])
 *     OlapTableScan(table=[[DEFAULT, KYLIN_SALES]], ctx=[], fields=[[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13]])
 *
 */

public class UnionTypeCastRule extends RelOptRule {

    public static final UnionTypeCastRule INSTANCE = new UnionTypeCastRule(operand(OlapUnionRel.class, any()),
            RelFactories.LOGICAL_BUILDER, "UnionTypeCastRule");

    public UnionTypeCastRule(RelOptRuleOperand operand, RelBuilderFactory relBuilderFactory, String description) {
        super(operand, relBuilderFactory, description);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        return !((SetOp) call.rel(0)).isHomogeneous(true);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        OlapUnionRel logicalUnion = call.rel(0);
        RelDataType rowType = logicalUnion.getRowType();
        List<RelNode> projects = new ArrayList<>();
        for (RelNode relNode : logicalUnion.getInputs()) {
            RelNode origin = ((HepRelVertex) relNode).getCurrentRel();
            OlapProjectRel castProject;

            if (origin instanceof OlapProjectRel) {
                // while origin is instance of OlapProjectRel, it will be replaced by the new one.
                OlapProjectRel project = (OlapProjectRel) origin;
                List<RexNode> castExpressions = RexUtil.generateCastExpressions(project.getCluster().getRexBuilder(),
                        rowType, project.getProjects());
                castExpressions = checkNullability(project.getCluster().getRexBuilder(), rowType, castExpressions);
                castProject = new OlapProjectRel(project.getCluster(), project.getTraitSet(), project.getInput(),
                        castExpressions, rowType);
            } else {
                // while origin is not a OlapProjectRel, a OlapProjectRel will be created and take the origin as input
                List<RexNode> castExpressions = RexUtil.generateCastExpressions(origin.getCluster().getRexBuilder(),
                        rowType, origin.getRowType());
                castExpressions = checkNullability(origin.getCluster().getRexBuilder(), rowType, castExpressions);
                castProject = new OlapProjectRel(origin.getCluster(), origin.getTraitSet(), origin, castExpressions,
                        rowType);
            }
            projects.add(castProject);
        }
        call.transformTo(
                new OlapUnionRel(logicalUnion.getCluster(), logicalUnion.getTraitSet(), projects, logicalUnion.all));
    }

    private List<RexNode> checkNullability(RexBuilder rexBuilder, RelDataType rowType, List<RexNode> castExpressions) {
        List<RexNode> newCastExpressions = new ArrayList<>();
        for (Pair<RelDataTypeField, RexNode> pair : Pair.zip(rowType.getFieldList(), castExpressions, true)) {
            if (pair.left.getType().isNullable() != pair.right.getType().isNullable()) {
                newCastExpressions.add(rexBuilder.makeAbstractCast(pair.left.getType(), pair.right));
            } else {
                newCastExpressions.add(pair.right);
            }
        }
        return newCastExpressions;
    }
}
